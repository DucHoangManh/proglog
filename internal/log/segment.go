package log

import (
	"fmt"
	api "github.com/DucHoangManh/proglog/api/v1"
	"github.com/golang/protobuf/proto"
	"os"
	"path"
)

// segment wraps the index and store to coordinate operations across the two
// flow : append new log to active segment -> segment write data to store and create a new index record
type segment struct {
	store      *store
	index      *index
	baseOffSet uint64
	nextOffSet uint64
	config     Config
}

func newSegment(dir string, baseOffSet uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffSet: baseOffSet,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffSet, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, err
	}
	if s.store, err = NewStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffSet, ".index")),
		os.O_CREATE|os.O_RDWR,
		0664)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		// EOF, empty file
		s.nextOffSet = baseOffSet
	} else {
		s.nextOffSet = baseOffSet + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffSet
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return
	}
	err = proto.Unmarshal(p, &api.Record{})
	_, pos, err := s.store.Append(p)
	if err != nil {
		return
	}
	if err = s.index.Write(uint32(s.nextOffSet-s.baseOffSet), pos); err != nil {
		return
	}
	s.nextOffSet++
	return cur, nil
}

// Read the relative position from the index and get record from store file
func (s *segment) Read(off uint64) (r *api.Record, err error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffSet))
	if err != nil {
		return
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return
	}
	r = &api.Record{}
	err = proto.Unmarshal(p, r)
	return
}

// IsMaxed check if current segment is full (index file or store file is full)
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close index file and store file of the current segment
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Remove remove the current segment and delete store and index file
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.file.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.file.Name()); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
