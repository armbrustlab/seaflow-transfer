package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/armbrustlab/seaflow-transfer/internal/fs"
	"golang.org/x/crypto/ssh/terminal"
)

const versionStr string = "v0.3.0"

var (
	srcRoot      string // SRCROOT
	dstRoot      string // DSTROOT
	srcAddress   string // SRCADDRESS
	dstAddress   string // DSTADDRESS
	sshPort      string // SSHPORT
	sshUser      string // SSHUSER
	sshPassword  string // SSHPASSWORD
	sshPublicKey string // SSHPUBLICKEY
	quiet        bool   // QUIET
	start        string // START
	verbose      bool   // VERBOSE
	version      bool   // VERSION
)
var t0 time.Time
var cmdname string = "seaflow-transfer"

func init() {
	initFlags()
	initEnvVars()
	if version {
		fmt.Printf("%v\n", versionStr)
		os.Exit(0)
	}
	if sshPassword == "" && (srcAddress != "" || dstAddress != "") {
		fmt.Printf("enter SSH password: ")
		b, err := terminal.ReadPassword(syscall.Stdin)
		if err != nil {
			log.Fatal(err)
		}
		sshPassword = string(b)
	}
	if start != "" {
		var err error
		t0, err = time.Parse(time.RFC3339, start)
		if err != nil {
			log.Fatalf("could not parse -start RFC3339 timestamp: %v", err)
		}
	}
}

func initFlags() {
	flagset := flag.NewFlagSet(cmdname, flag.ExitOnError)
	flagset.StringVar(&srcRoot, "srcRoot", "", "Root path of source")
	flagset.StringVar(&dstRoot, "dstRoot", "", "Root path of destination")
	flagset.StringVar(&srcAddress, "srcAddress", "", "Address of SFTP source")
	flagset.StringVar(&dstAddress, "dstAddress", "", "Address of SFTP destination")
	flagset.StringVar(&sshPort, "sshPort", "22", "SSH port")
	flagset.StringVar(&sshUser, "sshUser", "", "SSH user name")
	flagset.StringVar(&sshPublicKey, "sshPublicKey", "", "SSH public key file, overrides SSHPASSWORD")
	flagset.BoolVar(&quiet, "quiet", false, "Suppress informational logging")
	flagset.StringVar(&start, "start", "", "Earliest file timestamp to transfer as an RFC3339 string")
	flagset.BoolVar(&verbose, "verbose", false, "Enable debugging logs")
	flagset.BoolVar(&version, "version", false, "Display version and exit")

	flagset.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Transfer SeaFlow files between source and destination, which can be SFTP or local.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Will not transfer gzipped files, but will gzip before writing to destination.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "If using SFTP, the SSH password should be set in ENV as SSHPASSWORD.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Otherwise the password will be gathered from a prompt.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "All other options can be set in ENV as well, overriding CLI options.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "ENV variable names should be uppercased CLI option names.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Boolean option ENV vars should be set to 1 for true.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", cmdname)
		flagset.PrintDefaults()
	}
	args := make([]string, 0)
	if len(os.Args) > 1 {
		args = os.Args[1:]
	}
	err := flagset.Parse(args)
	if err != nil {
		panic(err)
	}
}

func initEnvVars() {
	val, ok := os.LookupEnv("SRCROOT")
	if ok {
		srcRoot = val
	}
	val, ok = os.LookupEnv("DSTROOT")
	if ok {
		dstRoot = val
	}
	val, ok = os.LookupEnv("SRCADDRESS")
	if ok {
		srcAddress = val
	}
	val, ok = os.LookupEnv("DSTADDRESS")
	if ok {
		dstAddress = val
	}
	val, ok = os.LookupEnv("SSHPORT")
	if ok {
		sshPort = val
	}
	val, ok = os.LookupEnv("SSHUSER")
	if ok {
		sshUser = val
	}
	val, ok = os.LookupEnv("SSHPASSWORD")
	if ok {
		sshPassword = val
	}
	val, ok = os.LookupEnv("QUIET")
	if ok && val == "1" {
		quiet = true
	}
	val, ok = os.LookupEnv("START")
	if ok {
		start = val
	}
	val, ok = os.LookupEnv("VERBOSE")
	if ok && val == "1" {
		verbose = true
	}
	val, ok = os.LookupEnv("VERSION")
	if ok && val == "1" {
		version = true
	}
}

func main() {
	debugLogger := log.New(os.Stderr, "", log.Ldate|log.Ltime)
	infoLogger := log.New(os.Stderr, "", log.Ldate|log.Ltime)
	errorLogger := log.New(os.Stderr, "", log.Ldate|log.Ltime)

	if !verbose || quiet {
		debugLogger.SetOutput(ioutil.Discard)
	}

	if quiet {
		infoLogger.SetOutput(ioutil.Discard)
	}

	t := &fs.Transfer{
		Srcroot:  srcRoot,
		Dstroot:  dstRoot,
		Debug:    debugLogger,
		Info:     infoLogger,
		Error:    errorLogger,
		Earliest: t0,
	}
	var err error
	if srcAddress != "" {
		addr := fmt.Sprintf("%v:%v", srcAddress, sshPort)
		t.Srcfs, err = fs.NewSftpfs(addr, sshUser, sshPassword, sshPublicKey)
		infoLogger.Printf("connected to %v as %v\n", addr, sshUser)
	} else {
		t.Srcfs, err = fs.NewLocalfs()
	}
	if err != nil {
		log.Fatal(err)
	}
	if dstAddress != "" {
		addr := fmt.Sprintf("%v:%v", dstAddress, sshPort)
		t.Dstfs, err = fs.NewSftpfs(addr, sshUser, sshPassword, sshPublicKey)
		infoLogger.Printf("connected to %v as %v\n", addr, sshUser)
	} else {
		t.Dstfs, err = fs.NewLocalfs()
	}
	if err != nil {
		log.Fatal(err)
	}

	err = t.CopySFLFiles()
	if err != nil {
		log.Fatal(err)
	}
	err = t.CopyEVTFiles()
	if err != nil {
		log.Fatal(err)
	}

	err = t.Close()
	if err != nil {
		log.Fatal(err)
	}
}
