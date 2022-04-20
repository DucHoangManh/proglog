package log

import (
	api "github.com/DucHoangManh/proglog/api/v1"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultStoreSize = 1024
	defaultIndexSize = 1024
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// Append the given record to current active segment
// after append, if current active segment is full, create a new active segment
func (l *Log) Append(record *api.Record) (off uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err = l.activeSegment.Append(record)
	if err != nil {
		return
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return
}

// Read returns the record at given offset
func (l *Log) Read(off uint64) (result *api.Record, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var segmentToRead *segment
	for _, segment := range l.segments {
		if segment.baseOffSet <= off && off < segment.nextOffSet {
			segmentToRead = segment
			break
		}
	}
	if segmentToRead == nil || segmentToRead.nextOffSet < off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return segmentToRead.Read(off)
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = defaultStoreSize
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = defaultIndexSize
	}
	log := &Log{
		Config: c,
		Dir:    dir,
	}
	return log, log.setup()
}

// Close all the current Log's segments
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove close the log and remove all of its data
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset clean all Log data
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// setup create all segments based on file in directory,
// if no file is present, create a new segment with initialize offset
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffSets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffSets = append(baseOffSets, off)
	}
	sort.Slice(baseOffSets, func(i, j int) bool {
		return baseOffSets[i] < baseOffSets[j]
	})

	for i := 0; i < len(baseOffSets); i++ {
		if err := l.newSegment(baseOffSets[i]); err != nil {
			return err
		}
		// base offset is duplicate for index and store so skip it
		i++
	}
	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	seg, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

func (l *Log) LowestOffset() (off uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffSet, nil
}

func (l *Log) HighestOffset() (off uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off = l.segments[len(l.segments)-1].nextOffSet
	if off == 0 {
		return
	}
	return off - 1, nil
}

// Truncate delete all old segment that have nextOffset smaller than given offset
// used to remove old data that we don't need any more
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var truncatedSegments []*segment
	for _, s := range l.segments {
		if s.nextOffSet <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		truncatedSegments = append(truncatedSegments, s)
	}
	l.segments = truncatedSegments
	return nil
}

//Reader returns an io.Reader to read the whole  log
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originReader{
			store: s.store,
			off:   0,
		}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (r *originReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}
