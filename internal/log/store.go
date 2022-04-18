package log

import (
	"bufio"
	"encoding/binary"
	_ "io"
	"os"
	"sync"
)

var enc = binary.BigEndian

// we store record length with lenWidth bytes
const lenWidth = 8

type store struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		file: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

func (s *store) Append(p []byte) (n, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// pos stand for the position that we can start to read data
	pos = s.size
	// firstly, write the length of the data to buffer instead of direct write to the file to reduce syscall
	// this length will be use later to know how many bytes to read
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write the length and data to file
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// we store record length with lenWidth bytes
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) (result []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// make sure that all the records are flushed to disk
	if err = s.buf.Flush(); err != nil {
		return nil, err
	}
	// first read the data size from pos with
	dataSize := make([]byte, lenWidth)
	if _, err = s.file.ReadAt(dataSize, int64(pos)); err != nil {
		return nil, err
	}
	// read (dataSize) bytes from the pos + lenWidth
	result = make([]byte, enc.Uint64(dataSize))
	if _, err = s.file.ReadAt(result, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return
}

// ReadAt call File's ReadAt method with mutex
func (s *store) ReadAt(p []byte, offset int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.file.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}
