package fs

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const nanoseconds = 1000000000

type StorageTestSuite struct {
	suite.Suite
	tmpDir string
	srcDir string
	dstDir string
	t      *Transfer
}

func (suite *StorageTestSuite) SetupTest() {
	tmpDir, err := ioutil.TempDir("", "fs-test-dir")
	if err != nil {
		panic(err)
	}
	suite.tmpDir = tmpDir
	suite.srcDir = filepath.Join(tmpDir, "src")
	err = os.Mkdir(suite.srcDir, os.ModeDir|0755)
	if err != nil {
		panic(err)
	}
	suite.dstDir = filepath.Join(tmpDir, "dst")
	srcfs, _ := NewLocalfs()
	dstfs, _ := NewLocalfs()
	suite.t = &Transfer{
		Srcroot: suite.srcDir,
		Dstroot: suite.dstDir,
		Srcfs:   srcfs,
		Dstfs:   dstfs,
		Info:    log.New(ioutil.Discard, "", 0),
	}
}

func (suite *StorageTestSuite) TeardownTest() {
	os.RemoveAll(suite.tmpDir)
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}

func (suite *StorageTestSuite) TestCopyFileLocalLocal() {
	testCopyFile(suite)
}

func testCopyFile(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	a := filepath.Join("2016_133", "2016-05-12T17-00-02-00-00")
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	makeFile(filepath.Join(suite.srcDir, a), "a")

	err := suite.t.CopyFile(filepath.Join(suite.srcDir, a), false)

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a), a+" copied")
	assert.Equal("a", readFile(filepath.Join(suite.dstDir, a)), a+" contents are correct")
	assert.Equal(
		mtime(filepath.Join(suite.srcDir, a)).UnixNano()/nanoseconds,
		mtime(filepath.Join(suite.dstDir, a)).UnixNano()/nanoseconds,
		a+" modtime updated",
	)
}

func (suite *StorageTestSuite) TestCopyFilegzLocalLocal() {
	testCopyFilegz(suite)
}

func testCopyFilegz(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	a := filepath.Join("2016_133", "2016-05-12T17-00-02-00-00")
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	makeFile(filepath.Join(suite.srcDir, a), "a")

	err := suite.t.CopyFile(filepath.Join(suite.srcDir, a), true)

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a+".gz"), a+" copied")
	assert.Equal("a", readFilegz(filepath.Join(suite.dstDir, a+".gz")), a+" contents are correct")
	assert.Equal(
		mtime(filepath.Join(suite.srcDir, a)).UnixNano(),
		mtime(filepath.Join(suite.dstDir, a+".gz")).UnixNano(),
		a+" modtime updated",
	)
	assert.Equal(
		mtime(filepath.Join(suite.srcDir, a)).UnixNano()/nanoseconds,
		mtimegz(filepath.Join(suite.dstDir, a+".gz")).UnixNano()/nanoseconds,
		a+" gzipped content modtime updated",
	)
}

func (suite *StorageTestSuite) TestCopyFileAlreadygzLocaLocal() {
	testCopyFileAlreadygz(suite)
}

func testCopyFileAlreadygz(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	a := filepath.Join("2016_133", "2016-05-12T17-00-02-00-00.gz")
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	makeFilegz(filepath.Join(suite.srcDir, a), "a")

	// Asking to gzip an already gzipped file shouldn't gzip it again
	err := suite.t.CopyFile(filepath.Join(suite.srcDir, a), true)

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a), a+" copied")
	assert.Equal("a", readFilegz(filepath.Join(suite.dstDir, a)), a+" contents are correct")
	assert.True(
		mtime(filepath.Join(suite.srcDir, a)).Equal(mtime(filepath.Join(suite.dstDir, a))),
		a+" modtime updated",
	)
}

func (suite *StorageTestSuite) TestCopySFLFilesNoMatchesLocaLocal() {
	testCopySFLFilesNoMatches(suite)
}
func testCopySFLFilesNoMatches(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	err := suite.t.CopySFLFiles()
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.True(dirNotExists(suite.dstDir), "dest directory not created")
}

func (suite *StorageTestSuite) TestCopyEVTFilesNoMatchesLocaLocal() {
	testCopyEVTFilesNoMatches(suite)
}

func testCopyEVTFilesNoMatches(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	err := suite.t.CopyEVTFiles()
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.True(dirNotExists(suite.dstDir), "dest directory not created")
}

func (suite *StorageTestSuite) TestCopySFLFilesLocalLocal() {
	testCopySFLFiles(suite)
}

func testCopySFLFiles(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	a := filepath.Join("2016_133", "a.sfl")
	b := filepath.Join("2016_134", "b.sfl")
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	mkdir(filepath.Join(suite.srcDir, "2016_134"))
	makeFile(filepath.Join(suite.srcDir, a), "a")
	makeFile(filepath.Join(suite.srcDir, b), "b")

	err := suite.t.CopySFLFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a), a+" copied")
	assert.FileExists(filepath.Join(suite.dstDir, b), b+" copied")
	assert.Equal("a", readFile(filepath.Join(suite.dstDir, a)), a+" contents are correct")
	assert.Equal("b", readFile(filepath.Join(suite.dstDir, b)), b+" contents are correct")

	// Change source files
	makeFile(filepath.Join(suite.srcDir, a), "aa")
	makeFile(filepath.Join(suite.srcDir, b), "bb")

	err = suite.t.CopySFLFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal("aa", readFile(filepath.Join(suite.dstDir, a)), a+" contents are correct")
	assert.Equal("bb", readFile(filepath.Join(suite.dstDir, b)), b+" contents are correct")
}

func (suite *StorageTestSuite) testCopyEVTFilesLocalLocal() {
	testCopyEVTFiles(suite)
}

func testCopyEVTFiles(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	a := filepath.Join("2016_133", "2016-05-12T17-00-02-00-00")
	b := filepath.Join("2016_133", "2016-05-12T17-00-05-00-00")
	c := filepath.Join("2016_134", "2016-05-13T00-00-35-00-00") // last file, should not get copied
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	mkdir(filepath.Join(suite.srcDir, "2016_134"))
	makeFile(filepath.Join(suite.srcDir, a), "a")
	makeFile(filepath.Join(suite.srcDir, b), "b")
	makeFile(filepath.Join(suite.srcDir, c), "c")

	err := suite.t.CopyEVTFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a+".gz"), a+" copied")
	assert.FileExists(filepath.Join(suite.dstDir, b+".gz"), b+" copied")
	assert.True(fileNotExists(filepath.Join(suite.dstDir, c+".gz")), c+" (last file) not copied")
	assert.Equal(
		"a",
		readFilegz(filepath.Join(suite.dstDir, a+".gz")),
		a+" contents are correct and file was not gzipped (again) in transit",
	)
	assert.Equal(
		"b",
		readFilegz(filepath.Join(suite.dstDir, b+".gz")),
		b+" contents are correct and have been gzipped in transit",
	)

	// Change source files
	makeFile(filepath.Join(suite.srcDir, a), "aa")
	makeFile(filepath.Join(suite.srcDir, b), "bb")

	err = suite.t.CopyEVTFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal("a", readFilegz(filepath.Join(suite.dstDir, a+".gz")), a+" content was not updated because it already exists")
	assert.Equal("b", readFilegz(filepath.Join(suite.dstDir, b+".gz")), b+" content was not updated because it already exists")
}

func chtimes(path string, atime time.Time, mtime time.Time) {
	err := os.Chtimes(path, atime, mtime)
	if err != nil {
		panic(err)
	}
}

func dirNotExists(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

func fileNotExists(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

func mkdir(path string) {
	err := os.Mkdir(path, os.ModeDir|0755)
	if err != nil {
		panic(err)
	}
}

func mtime(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	return info.ModTime()
}

func mtimegz(path string) time.Time {
	r, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	gzipr, err := gzip.NewReader(r)
	if err != nil {
		panic(err)
	}
	return gzipr.ModTime
}

func makeFile(path string, text string) {
	// for tests  set modification time back one second to properly test gzip
	// header modification time
	err := ioutil.WriteFile(path, []byte(text), 0755)
	if err != nil {
		panic(err)
	}
	mtime := mtime(path)
	mtime = mtime.Add(-1 * time.Second)
	chtimes(path, mtime, mtime)
}

func makeFilegz(path string, text string) {
	// for tests  set modification time back one second to properly test gzip
	// header modification time
	w, err := os.Create(path)
	mtime := mtime(path)
	mtime = mtime.Add(-1 * time.Second)

	gzipw := gzip.NewWriter(w)
	gzipw.ModTime = mtime
	_, err = gzipw.Write([]byte(text))
	if err != nil {
		panic(err)
	}

	err = gzipw.Close()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	chtimes(path, mtime, mtime)
}

func readFile(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func readFilegz(path string) string {
	r, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	gzipr, err := gzip.NewReader(r)
	if err != nil {
		panic(err)
	}
	data := make([]byte, 100)
	n, err := gzipr.Read(data)
	if err != nil && err != io.EOF {
		panic(err)
	}
	return string(data[:n])
}
