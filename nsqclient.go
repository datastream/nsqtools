package main

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"time"
)

// define internal write message struct
type WMessage struct {
	Topic string
	Body  interface{}
	Stat  chan error
}

// may be should add more info in writer
type Writer struct {
	MessageChan chan WMessage
	net.Conn
	islive bool
}

func NewWriter(addr string) *Writer {
	this := &Writer{
		MessageChan: make(chan WMessage),
	}
	this.islive = true
	go this.NewWriteLoop(addr)
	return this
}

// stop writerloop
func (this *Writer) Stop() {
	this.islive = false
	time.Sleep(time.Second)
	close(this.MessageChan)
}

// body should be []byte or [][]byte
func (this *Writer) Write(topic string, body interface{}) error {
	msg := WMessage{
		Topic: topic,
		Body:  body,
		Stat:  make(chan error),
	}
	if !this.islive {
		return errors.New("Write Stopped")
	}
	this.MessageChan <- msg
	err := <-msg.Stat
	if err != nil {
		if err.Error() != "Body not acceptable" {
			this.MessageChan <- msg
			err = <-msg.Stat
		}
	}
	return err
}

// if client can't connect nsqd, just exit loop. User should change nsqd_address or just recall NewWriteLoop()
func (this *Writer) NewWriteLoop(addr string) {
	go this.heartbeat()
	for {
		log.Println("Start WriteLoop")
		if this.islive {
			if err := this.writeloop(addr); err != nil {
				log.Println(err)
			}
		} else {
			break
		}
		log.Println("WriterLoop stopped")
	}
}

// write loop until tcp error
func (this *Writer) writeloop(addr string) error {
	var err error
	this.Conn, err = net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return err
	}
	defer this.Conn.Close()
	this.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(this.Conn),
		bufio.NewWriter(this.Conn))
	for {
		msg, ok := <-this.MessageChan
		isnop := false
		if !ok {
			break
		}
		var cmd *nsq.Command
		switch msg.Body.(type) {
		case []byte:
			cmd = nsq.Publish(msg.Topic, msg.Body.([]byte))
		case [][]byte:
			cmd, err = nsq.MultiPublish(msg.Topic, msg.Body.([][]byte))
		case error:
			cmd = nsq.Nop()
			isnop = true
		default:
			msg.Stat <- errors.New("Body not acceptable")
			continue
		}
		if err != nil {
			msg.Stat <- err
			break
		}
		if err = cmd.Write(rwbuf); err != nil {
			log.Println("write buf error", err)
			msg.Stat <- err
			break
		}
		if err = rwbuf.Flush(); err != nil {
			log.Println("flush buf error", err)
			msg.Stat <- err
			break
		}
		if isnop {
			msg.Stat <- nil
			continue
		}
		resp, err := nsq.ReadResponse(rwbuf)
		if err != nil {
			log.Println("failed to read response", err)
			msg.Stat <- err
			break
		}
		_, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			log.Println("unpack failed", err)
			msg.Stat <- err
			continue
		}
		if !bytes.Equal(data, []byte("OK")) &&
			!bytes.Equal(data, []byte("_heartbeat_")) {
			log.Println("response not ok", string(data))
			err = errors.New(string(data))
		}
		msg.Stat <- err
	}
	return nil
}

// send nop every 30s
func (this *Writer) heartbeat() {
	tick := time.Tick(time.Second * 30)
	for {
		<-tick
		if !this.islive {
			break
		}
		msg := WMessage{
			Topic: "",
			Body:  errors.New(""),
			Stat:  make(chan error),
		}
		this.MessageChan <- msg
		<-msg.Stat
	}
}
