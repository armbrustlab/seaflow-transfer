package fs

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type file interface {
	Close() error
	Read(b []byte) (int, error)
	Stat() (os.FileInfo, error)
	Write(b []byte) (int, error)
}

// Fs represents an abstract filesytem
type Fs interface {
	chtimes(path string, atime time.Time, mtime time.Time) error
	close() error
	create(path string) (file, error)
	glob(pattern string) (matches []string, err error)
	mkdirAll(path string) error
	open(path string) (file, error)
	rename(oldname, newname string) error
}

// Sftpfs provides methods to manipulate files on an SFTP server
type Sftpfs struct {
	client *sftp.Client
}

// NewSftpfs creates a new Sftpfs struct
func NewSftpfs(addr string, user string, pass string, publickey string) (Sftpfs, error) {
	client, err := newSftpClient(addr, user, pass, publickey)
	if err != nil {
		return Sftpfs{}, err
	}
	return Sftpfs{client: client}, nil
}

func (s Sftpfs) chtimes(path string, atime time.Time, mtime time.Time) error {
	return s.client.Chtimes(path, atime, mtime)
}

func (s Sftpfs) close() error {
	return s.client.Close()
}

func (s Sftpfs) create(path string) (file, error) {
	return s.client.Create(path)
}

func (s Sftpfs) glob(pattern string) (matches []string, err error) {
	return s.client.Glob(pattern)
}

func (s Sftpfs) mkdirAll(path string) error {
	return s.client.MkdirAll(path)
}

func (s Sftpfs) open(path string) (file, error) {
	return s.client.Open(path)
}

func (s Sftpfs) rename(oldname, newname string) error {
	return s.client.PosixRename(oldname, newname)
}

// Localfs provides methods to manipulate files local filesystem
type Localfs struct{}

// NewLocalfs creates a new Localfs struct
func NewLocalfs() (Localfs, error) {
	return Localfs{}, nil
}

func (l Localfs) chtimes(path string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(path, atime, mtime)
}

func (l Localfs) close() error {
	return nil
}

func (l Localfs) create(path string) (file, error) {
	return os.Create(path)
}

func (l Localfs) glob(pattern string) (matches []string, err error) {
	return filepath.Glob(pattern)
}

func (l Localfs) mkdirAll(path string) error {
	return os.MkdirAll(path, os.ModeDir|0755)
}

func (l Localfs) open(path string) (file, error) {
	return os.Open(path)
}

func (l Localfs) rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

// Transfer provides methods to copy SeaFlow data from a source to a destination
// location
type Transfer struct {
	Srcfs   Fs
	Srcroot string
	Dstfs   Fs
	Dstroot string
	Info    *log.Logger
	rand    *rand.Rand // for temp file names
}

// CopySFLFiles copies SFL files from source to destination. Files are
// identifed as <root>/<day-of-year-directory>/<filename>.
func (t *Transfer) CopySFLFiles() error {
	// Always copy all SFL files
	srcPattern := filepath.Join(t.Srcroot, "????_???", "*.sfl")
	srcFiles, err := t.Srcfs.glob(srcPattern)
	if err != nil {
		panic(err)
	}
	t.Info.Printf("found %v source SFL files\n", len(srcFiles))
	for _, path := range srcFiles {
		err := t.CopyFile(path, false)
		if err != nil {
			return fmt.Errorf("error while copying %v: %v", path, err)
		}
		t.Info.Printf("copied %v\n", path)
	}
	return nil
}

// CopyEVTFiles copies EVT files from source to destination. It gzips output
// files if not already compressed and skips files present in destination. Files
// are identifed as <root>/<day-of-year-directory>/<filename>. Files present in
// both source and destination, ignoring ".gz" extensions, are not copied. The
// most recent EVT file by filename timestamp is not copied since it may still
// be open for writing.
func (t *Transfer) CopyEVTFiles() error {
	// Transfer all EVT files except last (most recent)
	srcPattern := filepath.Join(t.Srcroot, "????_???", "????-??-??T??-??-??[\\-\\+]??-??")
	srcFiles, err := t.Srcfs.glob(srcPattern)
	if err != nil {
		panic(err)
	}
	t.Info.Printf("found %v source EVT files\n", len(srcFiles))
	if len(srcFiles) > 1 {
		// Copy all but the latest EVT file since it's most likely currently
		// being appended to. It's possible to identify the latest file here as
		// the last in the array after a lexicographical sort, which sorts
		// timestamped SeaFlow EVT files chronologically.
		sort.Strings(srcFiles)
		srcFiles = srcFiles[:len(srcFiles)-1]
		dstPattern := filepath.Join(t.Dstroot, "????_???", "????-??-??T??-??-??[\\-\\+]??-??")
		dstFiles, err := t.Dstfs.glob(dstPattern)
		if err != nil {
			panic(err)
		}
		dstFilesgz, err := t.Dstfs.glob(dstPattern + ".gz")
		if err != nil {
			panic(err)
		}
		dstFiles = append(dstFiles, dstFilesgz...)
		// Skip EVT files already present in destination
		present := make(map[string]bool)
		for _, path := range dstFiles {
			pathgz := path
			if filepath.Ext(path) == ".gz" {
				path = path[:len(path)-len(".gz")]
			} else {
				pathgz = pathgz + ".gz"
			}
			_, name := filepath.Split(path)
			present[name] = true
			_, namegz := filepath.Split(pathgz)
			present[namegz] = true
		}
		files := make([]string, 0)
		for _, path := range srcFiles {
			_, name := filepath.Split(path)
			if ok, _ := present[name]; !ok {
				files = append(files, path)
			}
		}

		t.Info.Printf("skipped %v duplicates and the most recent EVT file\n", len(srcFiles)-len(files))
		// Copy files
		for _, path := range files {
			err := t.CopyFile(path, true)
			if err != nil {
				return fmt.Errorf("error while copying %v: %v", path, err)
			}
			t.Info.Printf("copied %v\n", path)
		}
	}
	return nil
}

func (t *Transfer) tempName(filename string) string {
	if t.rand == nil {
		t.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 7)
	for i := range b {
		b[i] = charset[t.rand.Intn(len(charset))]
	}
	return "._seaflow-transfer_" + string(b) + "." + filename + "_"
}

// CopyFile copies one file from source to destination
func (t *Transfer) CopyFile(path string, gzipFlag bool) error {
	// Parse file path parts, handle gzip properly
	dir, filename := filepath.Split(path)
	_, doyDir := filepath.Split(filepath.Clean(dir))
	outdir := filepath.Join(t.Dstroot, doyDir)
	outpath := filepath.Join(outdir, filename)
	// To guarantee atomic file writes, create a temporary output file with
	// a name that won't get matched as an EVT file but with the final
	// target named embedded. This will get moved to the final path once
	// data is flushed.
	outpathtemp := filepath.Join(outdir, t.tempName(filename))
	if filepath.Ext(outpath) == ".gz" {
		gzipFlag = false
	}
	if gzipFlag {
		outpath = outpath + ".gz"
		outpathtemp = outpathtemp + ".gz"
	}

	// Make sure dir tree is ready to go
	err := t.Dstfs.mkdirAll(outdir)
	if err != nil {
		return fmt.Errorf("could not create dir %v: %v", outdir, err)
	}

	// Open input file
	in, err := t.Srcfs.open(path)
	if err != nil {
		return fmt.Errorf("could not open input file %v: %v", path, err)
	}
	defer in.Close()
	inStat, err := in.Stat()
	if err != nil {
		return fmt.Errorf("could not stat input file %v: %v", path, err)
	}

	// Copy file
	out, err := t.Dstfs.create(outpathtemp)
	if err != nil {
		return fmt.Errorf("could not create output file %v: %v", outpathtemp, err)
	}
	outbuf := bufio.NewWriter(out)
	var outgz *gzip.Writer
	if gzipFlag {
		outgz = gzip.NewWriter(outbuf)
		outgz.Name = filename
		// Set mod time for original file
		outgz.ModTime = inStat.ModTime()
		_, err := io.Copy(outgz, in)
		if err != nil {
			_ = out.Close() // free open file, don't care about errors
			return fmt.Errorf("could not copy and gzip %v to %v: %v", path, outpath, err)
		}
	} else {
		_, err := io.Copy(outbuf, in)
		if err != nil {
			_ = out.Close() // free open file, don't care about errors
			return fmt.Errorf("could not copy %v to %v: %v", path, outpath, err)
		}
	}

	// Flush and close everything
	if gzipFlag {
		err = outgz.Close()
		if err != nil {
			return err
		}
	}
	err = outbuf.Flush()
	if err != nil {
		return err
	}
	err = out.Close()
	if err != nil {
		return err
	}

	// Set modtime
	err = t.Dstfs.chtimes(outpathtemp, time.Now().Local(), inStat.ModTime())
	if err != nil {
		return fmt.Errorf("could not update mtime for output file %v: %v", outpathtemp, err)
	}

	// Rename from temp to final path
	err = t.Dstfs.rename(outpathtemp, outpath)
	if err != nil {
		return fmt.Errorf("could not perform final rename from %v to %v: %v", outpathtemp, outpath, err)
	}

	return nil
}

// Close releases any resources held
func (t *Transfer) Close() (err error) {
	srcerr := t.Srcfs.close()
	dsterr := t.Dstfs.close()
	switch {
	case srcerr != nil:
		err = srcerr
	case dsterr != nil:
		err = dsterr
	}
	return err
}

func newSftpClient(addr string, user string, pass string, publickey string) (client *sftp.Client, err error) {
	var auth ssh.AuthMethod
	if publickey != "" {
		key, err := ioutil.ReadFile(publickey)
		if err != nil {
			return client, fmt.Errorf("unable to read private key: %v", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return client, fmt.Errorf("unable to parse private key: %v", err)
		}
		auth = ssh.PublicKeys(signer)
	} else if pass != "" {
		auth = ssh.Password(pass)
	} else {
		return client, fmt.Errorf("must provide SSH password of public key")
	}
	sshConfig := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	conn, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return client, err
	}
	client, err = sftp.NewClient(conn)
	if err != nil {
		return client, err
	}
	return client, nil
}
