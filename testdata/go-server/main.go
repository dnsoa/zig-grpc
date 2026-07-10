// Package main is the interop peer for zig-grpc: a plaintext (h2c) grpc-go
// server exposing the standard Echo service on 127.0.0.1:50099.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/features/proto/echo"
)

type ecServer struct {
	pb.UnimplementedEchoServer
}

func (s *ecServer) UnaryEcho(_ context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	return &pb.EchoResponse{Message: req.Message}, nil
}

func (s *ecServer) ServerStreamingEcho(req *pb.EchoRequest, stream pb.Echo_ServerStreamingEchoServer) error {
	for i := 0; i < 3; i++ {
		if err := stream.Send(&pb.EchoResponse{Message: fmt.Sprintf("%s-%d", req.Message, i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ecServer) ClientStreamingEcho(stream pb.Echo_ClientStreamingEchoServer) error {
	var all string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.EchoResponse{Message: all})
		}
		if err != nil {
			return err
		}
		all += req.Message
	}
}

func (s *ecServer) BidirectionalStreamingEcho(stream pb.Echo_BidirectionalStreamingEchoServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.EchoResponse{Message: req.Message}); err != nil {
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:50099")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEchoServer(s, &ecServer{})
	log.Printf("echo server on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
