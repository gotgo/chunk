package chunk

import (
	"io"
	"path"
	"strconv"

	"github.com/gotgo/fw/me"
	"github.com/rlmcpherson/s3gof3r"
)

type S3Destination struct {
	S3Domain      string // "s3-us-west-2.amazonaws.com"
	BucketName    string // my.bucket.name
	AccessKey     string
	SecretKey     string
	SecurityToken string

	Folder string
}

func (d *S3Destination) Uri(filename string) string {
	return "http://" + path.Join(d.S3Domain, d.BucketName, d.path(filename))
}

func (d *S3Destination) Delete(filename string) error {
	bucket := d.bucket()
	return bucket.Delete(d.path(filename))
}

func (d *S3Destination) bucket() *s3gof3r.Bucket {
	keys := s3gof3r.Keys{}
	var err error

	if d.AccessKey == "" && d.SecretKey == "" && d.SecurityToken == "" {
		keys, err = s3gof3r.InstanceKeys() // get S3 keys
		if err != nil {
			panic("Failed to get aws access keys to S3")
		}
	} else {
		// setup
		keys = s3gof3r.Keys{
			AccessKey:     d.AccessKey,
			SecretKey:     d.SecretKey,
			SecurityToken: d.SecurityToken,
		}
	}
	s3 := s3gof3r.New(d.S3Domain, keys)

	return s3.Bucket(d.BucketName)
}

func (d *S3Destination) path(filename string) string {
	return path.Join(d.Folder, filename)
}

func (d *S3Destination) Create(filename string) (io.WriteCloser, error) {
	bucket := d.bucket()
	//	h := make(http.Header)
	//	h.Add("x-amz-meta-{0}", size)
	w, err := bucket.PutWriter(d.path(filename), nil, nil)
	if err != nil {
		return nil, me.Err(err, "bucket put writer fail")
	}

	return w, nil
}

func (d *S3Destination) Size(filename string) int64 {
	//totally lame, should just do an http Head
	bucket := d.bucket()
	r, h, err := bucket.GetReader(d.path(filename), nil)
	if err != nil {
		return -1
	}
	r.Close()
	length, err := strconv.ParseInt(h["Content-Length"][0], 10, 64)
	if err != nil {
		return -1
	}
	return length
}
