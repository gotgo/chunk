package chunk

import (
	"io"
	"path"

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
	bucket *s3gof3r.Bucket
}

func (d *S3Destination) Url(filename string) string {
	remoteFilePath := path.Join(d.Folder, filename)
	return "http://" + path.Join(d.S3Domain, d.BucketName, remoteFilePath)
}

func (d *S3Destination) Create(filename string) (io.WriteCloser, string, error) {
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

	d.bucket = s3.Bucket(d.BucketName)

	// specific
	remoteFilePath := path.Join(d.Folder, filename)

	w, err := d.bucket.PutWriter(remoteFilePath, nil, nil)
	if err != nil {
		return nil, "", me.Err(err, "bucket put writer fail")
	}

	uploadedTo := d.Url(filename)
	return w, uploadedTo, nil
}
