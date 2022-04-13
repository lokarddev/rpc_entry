package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"math"
	"net"
	"rpc_study/greet/greet.pb"
	"time"
)

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	s := grpc.NewServer()
	greet_pb.RegisterGreetServiceServer(s, &server{})

	if err = s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	//greet_pb.UnimplementedGreetServiceServer
	greet_pb.GreetServiceServer
	greet_pb.GreetService_GreetManyTimesServer
}

func (*server) Greet(ctx context.Context, request *greet_pb.GreetRequest) (*greet_pb.GreetResponse, error) {
	fmt.Printf("Greet function was evoked with %s \n", request)
	last := request.GetGreeting().GetLastName()
	first := request.GetGreeting().GetFirstName()

	result := fmt.Sprintf("Hello %s %s", first, last)
	res := &greet_pb.GreetResponse{Result: result}
	return res, nil
}

func (*server) GreetManyTimes(request *greet_pb.GreetManyTimesRequest, stream greet_pb.GreetService_GreetManyTimesServer) error {
	fmt.Printf("Greet many times function was evoked with %s \n", request)
	firstname := request.GetGreeting().GetFirstName()
	lastName := request.GetGreeting().GetFirstName()
	for i := 0; i < 10; i++ {
		response := &greet_pb.GreetManyTimesResponse{Result: fmt.Sprintf("%s %s requested a service %v times", firstname, lastName, i)}
		if err := stream.Send(response); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (*server) LongGreet(stream greet_pb.GreetService_LongGreetServer) error {
	fmt.Printf("Long greet times function was evoked with %s \n", stream)

	result := ""
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&greet_pb.LongGreetResponse{Result: result})
		}
		if err != nil {
			log.Fatalf("Error while reding client stream: %v", err)
			return err
		}
		firstName := request.GetGreeting().GetFirstName()
		result += "Hello " + firstName + "! "
	}
}

func (*server) GreetEveryone(stream greet_pb.GreetService_GreetEveryoneServer) error {
	fmt.Printf("Greet everyone times function was evoked with %s \n", stream)

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatal(err)
			return err
		}
		firstname := request.GetGreeting().GetFirstName()
		result := "Hello " + firstname + "! "
		if err = stream.Send(&greet_pb.GreetEveryoneResponse{Result: result}); err != nil {
			log.Fatal(err)
		}
	}
}

func (*server) SquareRoot(ctx context.Context, req *greet_pb.SquareRootRequest) (*greet_pb.SquareRootResponse, error) {
	fmt.Println("Square root run RPC")
	number := req.GetNumber()
	if number < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Receiver a negative number: %v", number)
	}
	return &greet_pb.SquareRootResponse{Number: math.Sqrt(float64(number))}, nil
}
