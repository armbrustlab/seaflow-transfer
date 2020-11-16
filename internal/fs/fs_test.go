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
		Debug:   log.New(ioutil.Discard, "", 0),
		Info:    log.New(ioutil.Discard, "", 0),
		Error:   log.New(ioutil.Discard, "", 0),
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
	assert.Equal("a", readFile(filepath.Join(suite.dstDir, a)), a+" content is correct")
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
	assert.Equal("a", readFilegz(filepath.Join(suite.dstDir, a+".gz")), a+" content is correct")
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

func (suite *StorageTestSuite) TestCopyFileAlreadygzLocalLocal() {
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
	assert.Equal("a", readFilegz(filepath.Join(suite.dstDir, a)), a+" content is correct")
	assert.True(
		mtime(filepath.Join(suite.srcDir, a)).Equal(mtime(filepath.Join(suite.dstDir, a))),
		a+" modtime updated",
	)
}

func (suite *StorageTestSuite) TestCopySFLFilesNoMatchesLocalLocal() {
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

func (suite *StorageTestSuite) TestCopyEVTFilesNoMatchesLocalLocal() {
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
	assert.Equal("a", readFile(filepath.Join(suite.dstDir, a)), a+" content is correct")
	assert.Equal("b", readFile(filepath.Join(suite.dstDir, b)), b+" content is correct")

	// Change source files
	makeFile(filepath.Join(suite.srcDir, a), "aa")
	makeFile(filepath.Join(suite.srcDir, b), "bb")

	err = suite.t.CopySFLFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal("aa", readFile(filepath.Join(suite.dstDir, a)), a+" content is correct")
	assert.Equal("bb", readFile(filepath.Join(suite.dstDir, b)), b+" content is correct")
}

func (suite *StorageTestSuite) TestCopySFLFilesWithTimeLocalLocal() {
	testCopySFLFilesWithTime(suite)
}

func testCopySFLFilesWithTime(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	suite.t.Earliest, _ = time.Parse(time.RFC3339, "2016-05-12T04:00:00Z")
	a := filepath.Join("2016_133", "a.sfl")
	b := filepath.Join("2016_133", "2016-05-12T03-00-00-00-00.sfl") // early file, should not get copied
	c := filepath.Join("2016_133", "2016-05-12T04-00-00-00-00.sfl")
	d := filepath.Join("2016_133", "2016-05-12T05-00-00-00-00.sfl")
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	makeFile(filepath.Join(suite.srcDir, a), "a")
	makeFile(filepath.Join(suite.srcDir, b), "b")
	makeFile(filepath.Join(suite.srcDir, c), "c")
	makeFile(filepath.Join(suite.srcDir, d), "d")

	err := suite.t.CopySFLFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.FileExists(filepath.Join(suite.dstDir, a), a+" copied")
	assert.True(fileNotExists(filepath.Join(suite.dstDir, b)), b+" early file not copied")
	assert.FileExists(filepath.Join(suite.dstDir, c), c+" copied")
	assert.FileExists(filepath.Join(suite.dstDir, d), d+" copied")
	assert.Equal("a", readFile(filepath.Join(suite.dstDir, a)), a+" content is correct")
	assert.Equal("c", readFile(filepath.Join(suite.dstDir, c)), c+" content is correct")
	assert.Equal("d", readFile(filepath.Join(suite.dstDir, d)), d+" content is correct")

	// Change source files
	makeFile(filepath.Join(suite.srcDir, a), "aa")
	makeFile(filepath.Join(suite.srcDir, c), "cc")
	makeFile(filepath.Join(suite.srcDir, d), "dd")

	err = suite.t.CopySFLFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal("aa", readFile(filepath.Join(suite.dstDir, a)), a+" content is correct")
	assert.True(fileNotExists(filepath.Join(suite.dstDir, b)), b+" early file not copied")
	assert.Equal("cc", readFile(filepath.Join(suite.dstDir, c)), c+" content is correct")
	assert.Equal("dd", readFile(filepath.Join(suite.dstDir, d)), d+" content is correct")
}

func (suite *StorageTestSuite) TestCopyEVTFilesLocalLocal() {
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
		a+" content is correct and file was not gzipped (again) in transit",
	)
	assert.Equal(
		"b",
		readFilegz(filepath.Join(suite.dstDir, b+".gz")),
		b+" content is correct and have been gzipped in transit",
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

func (suite *StorageTestSuite) TestCopyEVTFilesWithTimeLocalLocal() {
	testCopyEVTFilesWithTime(suite)
}

func testCopyEVTFilesWithTime(suite *StorageTestSuite) {
	assert := assert.New(suite.T())
	suite.t.Earliest, _ = time.Parse(time.RFC3339, "2016-05-12T04:00:00Z")
	a := filepath.Join("2016_133", "2016-05-12T03-00-02-00-00") // early file, should not get copied
	b := filepath.Join("2016_133", "2016-05-12T04-00-05-00-00")
	c := filepath.Join("2016_133", "2016-05-12T05-00-05-00-00")
	d := filepath.Join("2016_133", "2016-05-12T06-00-05-00-00") // last file, should not get copied
	mkdir(filepath.Join(suite.srcDir, "2016_133"))
	makeFile(filepath.Join(suite.srcDir, a), "a")
	makeFile(filepath.Join(suite.srcDir, b), "b")
	makeFile(filepath.Join(suite.srcDir, c), "c")
	makeFile(filepath.Join(suite.srcDir, d), "d")

	err := suite.t.CopyEVTFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.True(fileNotExists(filepath.Join(suite.dstDir, a+".gz")), a+" early file not copied")
	assert.FileExists(filepath.Join(suite.dstDir, b+".gz"), b+" copied")
	assert.FileExists(filepath.Join(suite.dstDir, c+".gz"), c+" copied")
	assert.True(fileNotExists(filepath.Join(suite.dstDir, d+".gz")), d+" last file not copied")
	assert.Equal(
		"b",
		readFilegz(filepath.Join(suite.dstDir, b+".gz")),
		b+" content is correct and have been gzipped in transit",
	)
	assert.Equal(
		"c",
		readFilegz(filepath.Join(suite.dstDir, c+".gz")),
		c+" content is correct and have been gzipped in transit",
	)

	// Change source files
	makeFile(filepath.Join(suite.srcDir, b), "bb")
	makeFile(filepath.Join(suite.srcDir, c), "cc")

	err = suite.t.CopyEVTFiles()

	assert.Nil(err)
	if err != nil {
		return
	}
	assert.True(fileNotExists(filepath.Join(suite.dstDir, a+".gz")), a+" early file not copied")
	assert.Equal(
		"b",
		readFilegz(filepath.Join(suite.dstDir, b+".gz")),
		b+" content was not updated because it already exists",
	)
	assert.Equal(
		"c",
		readFilegz(filepath.Join(suite.dstDir, c+".gz")),
		c+" content was not updated because it already exists",
	)
	assert.True(fileNotExists(filepath.Join(suite.dstDir, d+".gz")), d+" last file not copied")
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

func Test_timeFromFilename(t *testing.T) {
	timeAnswer, _ := time.Parse(time.RFC3339, "2019-12-06T22:58:10Z")
	timeAnswerFrac, _ := time.Parse(time.RFC3339, "2019-12-06T22:58:10.3Z")
	type args struct {
		fn string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name:    "correct with no gz",
			args:    args{fn: "2019-12-06T22-58-10+00-00"},
			want:    timeAnswer,
			wantErr: false,
		},
		{
			name:    "correct SFL",
			args:    args{fn: "2019-12-06T22-58-10+00-00.sfl"},
			want:    timeAnswer,
			wantErr: false,
		},
		{
			name:    "correct with gz",
			args:    args{fn: "2019-12-06T22-58-10+00-00.gz"},
			want:    timeAnswer,
			wantErr: false,
		},
		{
			name:    "correct with folders",
			args:    args{fn: "some/directory/path/2019-12-06T22-58-10+00-00"},
			want:    timeAnswer,
			wantErr: false,
		},
		{
			name:    "correct with non-UTC TZ",
			args:    args{fn: "2019-12-06T22-58-10+07-00"},
			want:    timeAnswer,
			wantErr: false,
		},
		{
			name:    "correct with fractional seconds",
			args:    args{fn: "2019-12-06T22-58-10.3+00-00"},
			want:    timeAnswerFrac,
			wantErr: false,
		},
		{
			name:    "incorrect with no gz",
			args:    args{fn: "2019-12-06Ta22-58-10+00-00"},
			wantErr: true,
		},
		{
			name:    "incorrect with gz",
			args:    args{fn: "2019-12-06Ta22-58-10+00-00.gz"},
			wantErr: true,
		},
		{
			name:    "incorrect with folders",
			args:    args{fn: "some/directory/path/201a9-12-06T22-58-10+00-00"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timeFromFilename(tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("timeFromFilename() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("timeFromFilename() = %v, want %v", got, tt.want)
			}
		})
	}
}
