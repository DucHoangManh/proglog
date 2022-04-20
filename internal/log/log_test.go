package log

import (
	"fmt"
	api "github.com/DucHoangManh/proglog/api/v1"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

type scenarios map[string]func(t *testing.T, l *Log)

func TestLog(t *testing.T) {
	ss := scenarios{
		"append and read a record succeeds": testAppendAndRead,
		"offset out of range error":         testOutOfRangeRead,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	}
	for s, fn := range ss {
		t.Run(s, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendAndRead(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	readResult, err := log.Read(0)
	require.NoError(t, err)
	require.Equal(t, readResult.Value, record.Value)
}

func testOutOfRangeRead(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, l *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())
	lowest, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)

	highest, err := l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)
	n, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)
	lowest, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)
	highest, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)
}

func testReader(t *testing.T, l *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := l.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
}

func testTruncate(t *testing.T, l *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	readResult, _ := l.Read(1)
	fmt.Println(readResult)
	err := l.Truncate(1)
	require.NoError(t, err)

	_, err = l.Read(1)
	require.Error(t, err)
}
