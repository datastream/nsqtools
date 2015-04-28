package main

import (
	"bufio"
	"github.com/bitly/go-nsq"
	"gopkg.in/fsnotify.v1"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type FileList struct {
	Files        map[string]bool
	FileDescribe map[string]*os.File
	sync.Mutex
}

func (f *FileList) Update(e fsnotify.Event) bool {
	switch e.Op {
	case fsnotify.Remove:
		f.Lock()
		delete(f.Files, e.Name)
		f.Unlock()
	case fsnotify.Write:
		if _, ok := f.Files[e.Name]; !ok {
			f.Lock()
			f.Files[e.Name] = true
			f.Unlock()
			return true
		}
	default:
	}
	return false
}
func (f *FileList) ReadLog(file string, topic string, w *nsq.Producer, exitchan chan int) {
	f.Lock()
	fd, err := os.Open(file)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	f.FileDescribe[file] = fd
	f.Unlock()
	_, err = fd.Seek(0, 2)
	if err != nil {
		return
	}
	reader := bufio.NewReader(fd)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(time.Second * 10)
			line, err = reader.ReadString('\n')
		}
		if err == io.EOF {
			log.Println("READ EOF")
			f.Lock()
			if _, ok := f.Files[file]; !ok {
				break
			}
			if f.FileDescribe[file] != fd {
				break
			}
			f.Unlock()
			continue
		}
		err = w.Publish(topic, []byte(line))
		if err != nil {
			log.Println("NSQ writer", err)
		}
		select {
		case <-exitchan:
			return
		default:
		}
	}
}
