package server

import (
	"context"
	api "github.com/DucHoangManh/proglog/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (res *api.ProduceResponse, err error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return
	}
	res = &api.ProduceResponse{Offset: offset}
	return
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (res *api.ConsumeResponse, err error) {
	rec, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return
	}
	res = &api.ConsumeResponse{Record: rec}
	return
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange: // in case get all the log, continue until new log append
				continue
			default:
				return err
			}
			if err := stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
