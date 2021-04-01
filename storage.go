package makeless_go_storage_amazon_s3

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/h2non/filetype"
	"github.com/h2non/filetype/types"
	"sync"
)

type Storage struct {
	Bucket string
	Config aws.Config

	session *session.Session
	*sync.RWMutex
}

func (storage *Storage) GetBucket() string {
	storage.RLock()
	defer storage.RUnlock()

	return storage.Bucket
}

func (storage *Storage) GetConfig() aws.Config {
	storage.RLock()
	defer storage.RUnlock()

	return storage.Config
}

func (storage *Storage) GetSession() *session.Session {
	storage.RLock()
	defer storage.RUnlock()

	return storage.session
}

func (storage *Storage) setSession(session *session.Session) {
	storage.Lock()
	defer storage.Unlock()

	storage.session = session
}

func (storage *Storage) Init() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: storage.GetConfig(),
	}))

	storage.setSession(sess)
	return nil
}

func (storage *Storage) Write(filepath string, data []byte) error {
	var err error
	var fileType types.Type
	var uploader = s3manager.NewUploader(storage.GetSession())

	if fileType, err = filetype.Match(data); err != nil {
		return err
	}

	if fileType == filetype.Unknown {
		return fmt.Errorf("unknown file type")
	}

	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(storage.GetBucket()),
		Key:         aws.String(filepath),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(fileType.MIME.Value),
	}); err != nil {
		return err
	}

	return nil
}

func (storage *Storage) Read(filepath string) ([]byte, error) {
	var downloader = s3manager.NewDownloader(storage.GetSession())
	var buf = new(aws.WriteAtBuffer)

	if _, err := downloader.Download(
		buf,
		&s3.GetObjectInput{
			Bucket: aws.String(storage.GetBucket()),
			Key:    aws.String(filepath),
		},
	); err != nil {
		return nil, err
	}

	return nil, nil
}

func (storage *Storage) Exists(filepath string) (bool, error) {
	_, err := s3.New(storage.GetSession()).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(storage.GetBucket()),
		Key:    aws.String(filepath),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				return false, nil
			default:
				return false, err
			}
		}

		return false, err
	}

	return true, nil
}

func (storage *Storage) Remove(filepath string) error {
	return fmt.Errorf("remove not supported yet")
}
