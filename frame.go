// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package geocube

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
)

const defaultBufSize = 128
const maxFrameSize = 256 * 1024 * 1024

type frameHeader struct {
	version       int
	flags         byte
	stream        int
	length        int
	//customPayload map[string][]byte
	warnings      []string
}

// a framer is responsible for reading, writing and parsing frames on a single stream
type framer struct {
	fReader io.Reader
	fWriter io.Writer

	proto byte
	headSize int
	// if this frame was read then the header will be here
	header *frameHeader

	// holds a ref to the whole byte slice for rbuf so that it can be reset to
	// 0 after a read.
	readBuffer []byte

	rbuf []byte
	wbuf []byte
}

func (f *framer) setLength(length int) {
  //FIXME: Why here?
	p := 4

	f.wbuf[p+0] = byte(length >> 24)
	f.wbuf[p+1] = byte(length >> 16)
	f.wbuf[p+2] = byte(length >> 8)
	f.wbuf[p+3] = byte(length)
}

func (f *framer) finishWrite() error {
	if len(f.wbuf) > maxFrameSize {
		// huge app frame, lets remove it so it doesn't bloat the heap
		f.wbuf = make([]byte, defaultBufSize)
		return ErrFrameTooBig
	}

	length := len(f.wbuf) - f.headSize
	f.setLength(length) //What the fuck is this doing?

	_, err := f.fWriter.Write(f.wbuf)
	if err != nil {
		return err
	}

	return nil
}


// reads a frame form the wire into the framers buffer
func (f *framer) readFrame(head *frameHeader) error {
	if head.length < 0 {
		return fmt.Errorf("frame body length can not be less than 0: %d", head.length)
	} else if head.length > maxFrameSize {
		// need to free up the connection to be used again ????????????
		_, err := io.CopyN(ioutil.Discard, f.fReader, int64(head.length))
		if err != nil {
			return fmt.Errorf("error whilst trying to discard frame with invalid length: %v", err)
		}
		return ErrFrameTooBig
	}

	if cap(f.readBuffer) >= head.length {
		f.rbuf = f.readBuffer[:head.length]
	} else {
		f.readBuffer = make([]byte, head.length)
		f.rbuf = f.readBuffer
	}

	// assume the underlying reader takes care of timeouts and retries
	n, err := io.ReadFull(f.r, f.rbuf)
	if err != nil {
		return fmt.Errorf("unable to read frame body: read %d/%d bytes: %v", n, head.length, err)
	}

	f.header = head
	return nil
}


func (f *framer) readByte() byte {
	if len(f.rbuf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read byte require 1 got: %d", len(f.rbuf)))
	}

	b := f.rbuf[0]
	f.rbuf = f.rbuf[1:]
	return b
}

func (f *framer) readInt() (n int) {
	if len(f.rbuf) < 4 {
		panic(fmt.Errorf("not enough bytes in buffer to read int require 4 got: %d", len(f.rbuf)))
	}

	n = int(int32(f.rbuf[0])<<24 | int32(f.rbuf[1])<<16 | int32(f.rbuf[2])<<8 | int32(f.rbuf[3]))
	f.rbuf = f.rbuf[4:]
	return
}

func (f *framer) readShort() (n uint16) {
	if len(f.rbuf) < 2 {
		panic(fmt.Errorf("not enough bytes in buffer to read short require 2 got: %d", len(f.rbuf)))
	}
	n = uint16(f.rbuf[0])<<8 | uint16(f.rbuf[1])
	f.rbuf = f.rbuf[2:]
	return
}

func (f *framer) readLong() (n int64) {
	if len(f.rbuf) < 8 {
		panic(fmt.Errorf("not enough bytes in buffer to read long require 8 got: %d", len(f.rbuf)))
	}
	n = int64(f.rbuf[0])<<56 | int64(f.rbuf[1])<<48 | int64(f.rbuf[2])<<40 | int64(f.rbuf[3])<<32 |
		int64(f.rbuf[4])<<24 | int64(f.rbuf[5])<<16 | int64(f.rbuf[6])<<8 | int64(f.rbuf[7])
	f.rbuf = f.rbuf[8:]
	return
}

func (f *framer) readString() (s string) {
	size := f.readShort()

	if len(f.rbuf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read string require %d got: %d", size, len(f.rbuf)))
	}

	s = string(f.rbuf[:size])
	f.rbuf = f.rbuf[size:]
	return
}

func (f *framer) readLongString() (s string) {
	size := f.readInt()

	if len(f.rbuf) < size {
		panic(fmt.Errorf("not enough bytes in buffer to read long string require %d got: %d", size, len(f.rbuf)))
	}

	s = string(f.rbuf[:size])
	f.rbuf = f.rbuf[size:]
	return
}

func (f *framer) readUUID() *UUID {
	if len(f.rbuf) < 16 {
		panic(fmt.Errorf("not enough bytes in buffer to read uuid require %d got: %d", 16, len(f.rbuf)))
	}

	// TODO: how to handle this error, if it is a uuid, then sureley, problems?
	u, _ := UUIDFromBytes(f.rbuf[:16])
	f.rbuf = f.rbuf[16:]
	return &u
}

func (f *framer) readStringList() []string {
	size := f.readShort()

	l := make([]string, size)
	for i := 0; i < int(size); i++ {
		l[i] = f.readString()
	}

	return l
}

func (f *framer) readBytesInternal() ([]byte, error) {
	size := f.readInt()
	if size < 0 {
		return nil, nil
	}

	if len(f.rbuf) < size {
		return nil, fmt.Errorf("not enough bytes in buffer to read bytes require %d got: %d", size, len(f.rbuf))
	}

	l := f.rbuf[:size]
	f.rbuf = f.rbuf[size:]

	return l, nil
}

func (f *framer) readBytes() []byte {
	l, err := f.readBytesInternal()
	if err != nil {
		panic(err)
	}

	return l
}

func (f *framer) readShortBytes() []byte {
	size := f.readShort()
	if len(f.rbuf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read short bytes: require %d got %d", size, len(f.rbuf)))
	}

	l := f.rbuf[:size]
	f.rbuf = f.rbuf[size:]

	return l
}

func (f *framer) readInet() (net.IP, int) {
	if len(f.rbuf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet size require %d got: %d", 1, len(f.rbuf)))
	}

	size := f.rbuf[0]
	f.rbuf = f.rbuf[1:]

	if !(size == 4 || size == 16) {
		panic(fmt.Errorf("invalid IP size: %d", size))
	}

	if len(f.rbuf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet require %d got: %d", size, len(f.rbuf)))
	}

	ip := make([]byte, size)
	copy(ip, f.rbuf[:size])
	f.rbuf = f.rbuf[size:]

	port := f.readInt()
	return net.IP(ip), port
}

func (f *framer) readConsistency() Consistency {
	return Consistency(f.readShort())
}

func (f *framer) readStringMap() map[string]string {
	size := f.readShort()
	m := make(map[string]string)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readString()
		m[k] = v
	}

	return m
}

func (f *framer) readBytesMap() map[string][]byte {
	size := f.readShort()
	m := make(map[string][]byte)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readBytes()
		m[k] = v
	}

	return m
}

func (f *framer) readStringMultiMap() map[string][]string {
	size := f.readShort()
	m := make(map[string][]string)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readStringList()
		m[k] = v
	}

	return m
}

func (f *framer) writeByte(b byte) {
	f.wbuf = append(f.wbuf, b)
}

func appendBytes(p []byte, d []byte) []byte {
	if d == nil {
		return appendInt(p, -1)
	}
	p = appendInt(p, int32(len(d)))
	p = append(p, d...)
	return p
}

func appendShort(p []byte, n uint16) []byte {
	return append(p,
		byte(n>>8),
		byte(n),
	)
}

func appendInt(p []byte, n int32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func appendLong(p []byte, n int64) []byte {
	return append(p,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

// these are protocol level binary types
func (f *framer) writeInt(n int32) {
	f.wbuf = appendInt(f.wbuf, n)
}

func (f *framer) writeShort(n uint16) {
	f.wbuf = appendShort(f.wbuf, n)
}

func (f *framer) writeLong(n int64) {
	f.wbuf = appendLong(f.wbuf, n)
}

func (f *framer) writeString(s string) {
	f.writeShort(uint16(len(s)))
	f.wbuf = append(f.wbuf, s...)
}

func (f *framer) writeLongString(s string) {
	f.writeInt(int32(len(s)))
	f.wbuf = append(f.wbuf, s...)
}

func (f *framer) writeUUID(u *UUID) {
	f.wbuf = append(f.wbuf, u[:]...)
}

func (f *framer) writeStringList(l []string) {
	f.writeShort(uint16(len(l)))
	for _, s := range l {
		f.writeString(s)
	}
}
