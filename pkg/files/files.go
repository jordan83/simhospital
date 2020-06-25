// Package files supports reading and writing files from local directories or GCS.
package files

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const gcsBucketPrefix = "gs://"

// File represents a file, either local or remote.
type File interface {
	Read() ([]byte, error)
	Name() string
	FullPath() string
}

// List lists files in the directory specified by the path.
func List(path string) ([]File, error) {
	if strings.HasPrefix(path, gcsBucketPrefix) {
		return listGCSFiles(path)
	}
	return listLocalFiles(path)
}

// Read reads the file specified by the path.
func Read(path string) ([]byte, error) {
	if strings.HasPrefix(path, gcsBucketPrefix) {
		return readGCSFile(path)
	}
	return readLocalFile(path)
}

func readGCSFile(path string) ([]byte, error) {
	f, err := listGCSFiles(path)
	if err != nil {
		return nil, err
	}
	if len(f) != 1 {
		return nil, fmt.Errorf("%s does not identify a file", path)
	}
	return f[0].Read()
}

func listGCSFiles(path string) ([]File, error) {
	b, prefix, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bucket := c.Bucket(b)
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	var files []File
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		files = append(files, gcsFile{prefix, bucket.Object(attrs.Name)})
	}
	return files, nil
}

func parseGCSPath(path string) (string, string, error) {
	if !strings.HasPrefix(path, gcsBucketPrefix) {
		return "", "", fmt.Errorf("GCS path has an invalid format: %s", path)
	}
	p := strings.TrimPrefix(path, gcsBucketPrefix)
	i := strings.Index(p, "/")
	if i == -1 {
		return p, "", nil
	}
	return p[:i], p[i+1:], nil
}

type gcsFile struct {
	prefix string
	object *storage.ObjectHandle
}

func (f gcsFile) Name() string {
	return strings.TrimPrefix(f.object.ObjectName(), fmt.Sprintf("%s/", f.prefix))
}

func (f gcsFile) FullPath() string {
	return f.object.ObjectName()
}

func (f gcsFile) Read() ([]byte, error) {
	ctx := context.Background()
	r, err := f.object.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(r)
	r.Close()
	return b, err
}

func readLocalFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("can't read local file; %s is a directory", path)
	}
	return ioutil.ReadFile(path)
}

func listLocalFiles(path string) ([]File, error) {
	dirFiles, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []File
	for _, f := range dirFiles {
		if f.IsDir() {
			continue
		}
		files = append(files, localFile{path, f.Name()})
	}
	return files, nil
}

type localFile struct {
	dirName  string
	fileName string
}

func (f localFile) Name() string {
	return f.fileName
}

func (f localFile) FullPath() string {
	return path.Join(f.dirName, f.fileName)
}

func (f localFile) Read() ([]byte, error) {
	return ioutil.ReadFile(f.FullPath())
}
