package main

import (
	"fmt"
	"os"
	"path"
	"time"
	"strconv"
	"github.com/pkg/errors"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/pborman/uuid"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/logging/debug"
)

// Mover is an Ali data mover
type Mover struct {
	name   string
	aliclient *oss.Client
	bucket string
	prefix string
	iPartsize int64
	iRoutines int
}

// AliMover returns a new *Mover
func AliMover(aliclient *oss.Client, archiveID uint32, bucket string, prefix string,iPartsize int64,iRoutines int) *Mover {
	return &Mover{
		name:   fmt.Sprintf("ali-%d", archiveID),
		aliclient: aliclient,
		bucket: bucket,
		prefix: prefix,
		iPartsize: iPartsize,
		iRoutines: iRoutines,
	}
}

func newFileID() string {
	return uuid.New()
}

func (m *Mover) destination(id string) string {
	return path.Join(m.prefix,
		id)
}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	debug.Printf("%s started", m.name)
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("alitrigger:%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	
	if m.aliclient != nil {
		debug.Printf("aliclient:%s",m.aliclient)
	}

	rate.Mark(1)
	start := time.Now()

	src, err := os.Open(action.PrimaryPath())
	if err != nil {
		return errors.Wrapf(err, "%s: open failed", action.PrimaryPath())
	}
	defer src.Close()

	fi, err := src.Stat()
	if err != nil {
		return errors.Wrap(err, "stat failed")
	}

	fileID := newFileID()


	isExist, err := m.aliclient.IsBucketExist(m.bucket)
	if err != nil {
		debug.Printf("check bucket %s exist failed",m.bucket)
	}

	if !isExist {
		err = m.aliclient.CreateBucket(m.bucket)
		if err != nil {
			debug.Printf("create bucket %s failed",m.bucket)
			return errors.Wrap(err, "no bucket")
		}
	}

	bucket, err := m.aliclient.Bucket(m.bucket)
        if err != nil {
        }

        err = bucket.UploadFile(m.destination(fileID), action.PrimaryPath(), m.iPartsize*1024*1024, oss.Routines(m.iRoutines))
        if err != nil {
		debug.Printf("Upload files %s to %s failed, m.iPartsize:%d, m.iRoutines:%d,because err:%s",action.PrimaryPath(),m.destination(fileID),m.iPartsize,m.iRoutines,err)
		return errors.Wrap(err, "upload failed")
        }


	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s", m.name, action.ID(), fi.Size(),
		time.Since(start),
		action.PrimaryPath(),
		m.destination(fileID))
	action.SetFileID([]byte(fileID))
	action.SetActualLength(uint64(fi.Size()))
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)

	start := time.Now()
	if action.FileID() == nil {
		return errors.Errorf("Missing file_id on action %d", action.ID())
	}

	srcObj := m.destination(string(action.FileID()))
	
	bucket, err := m.aliclient.Bucket(m.bucket)
        if err != nil {
		return errors.Wrap(err, "bucket doesn't exist")
        }
	
	//get file size from ali
	meta, err := bucket.GetObjectDetailedMeta(srcObj)
	if err != nil {
		debug.Printf("get file:%s meta from ali failed",srcObj)
		return errors.Wrap(err, "get file meta from ali failed")
	}

	n, err := strconv.ParseInt(meta.Get("Content-Length"),10,64)
	if err != nil {
		debug.Printf("get file:%s size from ali failed",srcObj)
		return errors.Wrap(err, "get file size from ali failed")
	}

	debug.Printf("succeed to get file:%s size from ali, the size is %d",srcObj,n)

	dstPath := action.WritePath()
	dst, err := os.OpenFile(dstPath, os.O_WRONLY, 0644)
	if err != nil {
		return errors.Errorf("Couldn't open %s for write: %s", dstPath, err)
	}
	defer dst.Close()

	err = bucket.DownloadFile(srcObj, dstPath, m.iPartsize*1024*1024, oss.Routines(m.iRoutines))
	if err != nil {
		debug.Printf("DownloadFile failed from %s to %s",srcObj,dstPath)
		return errors.Wrap(err, "DownloadFile failed")
	}

	debug.Printf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(),n,
		time.Since(start),
		srcObj,
		action.PrimaryPath())
	action.SetActualLength(uint64(n))
	
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	if action.FileID() == nil {
		return errors.New("Missing file_id")
	}

	bucket, err := m.aliclient.Bucket(m.bucket)
        if err != nil {
		return errors.Wrap(err, "bucket doesn't exist")
        }

	err = bucket.DeleteObject(m.destination(string(action.FileID())))
	if err != nil {
		debug.Printf("remove file with ID:%s failed",m.destination(string(action.FileID())))
		return errors.Wrap(err, "remove file failed")
	}


	return nil
	
}
