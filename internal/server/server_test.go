package server

import (
	api "github.com/DucHoangManh/proglog/api/v1"
	"github.com/DucHoangManh/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {

}

func setupTest(t *testing.T, fn func(config *Config)) (client api.LogClient, c *Config, tearDown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	c = &Config{CommitLog: clog}
	if fn != nil {
		fn(c)
	}
	server, err := NewGRPCServer(c)
	require.NoError(t, err)
	go func() {
		_ = server.Serve(l)
	}()

	client := api.NewLogClient(cc)
	return client, c, func() {
		server.Stop()
		_ = cc.Close()
		_ = l.Close()
	}
}
