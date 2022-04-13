package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	greet_pb "rpc_study/greet/greet.pb"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := greet_pb.NewGreetServiceClient(conn)

	//doUnary(c)
	//doServerStreaming(c)
	//doClientStreaming(c)
	//doBiDiStreaming(c)
	//doErrorUnary(c)
	doUnaryWithDeadline(c, time.Second*3)
	doUnaryWithDeadline(c, time.Second*6)
}

func doUnary(c greet_pb.GreetServiceClient) {
	fmt.Println("Start unary")
	req := &greet_pb.GreetRequest{Greeting: &greet_pb.Greeting{
		FirstName: "lokard",
		LastName:  "deepmaker",
	},
	}
	response, _ := c.Greet(context.Background(), req)

	fmt.Println(response)
}

func doServerStreaming(c greet_pb.GreetServiceClient) {
	fmt.Println("Start server streaming")
	input := &greet_pb.GreetManyTimesRequest{Greeting: &greet_pb.Greeting{
		FirstName: "lokard",
		LastName:  "deepmaker",
	}}
	response, _ := c.GreetManyTimes(context.Background(), input)
	for {
		msg, err := response.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(msg)
	}
}

func doClientStreaming(c greet_pb.GreetServiceClient) {
	fmt.Println("Client server streaming")
	request := []*greet_pb.LongGreetRequest{
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "lokard",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "deepmanker",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "jesteez",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "ja",
				LastName:  "",
			},
		},
	}

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	for _, r := range request {
		err = stream.Send(r)
		time.Sleep(time.Second * 2)
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response)
}

func doBiDiStreaming(c greet_pb.GreetServiceClient) {
	fmt.Println("BiDi server streaming")

	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatal(err)
		return
	}

	request := []*greet_pb.GreetEveryoneRequest{
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "lokard",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "deepmanker",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "jesteez",
				LastName:  "",
			},
		},
		{
			Greeting: &greet_pb.Greeting{
				FirstName: "ja",
				LastName:  "",
			},
		},
	}

	waitCh := make(chan struct{})

	go func() {
		for _, r := range request {
			err = stream.Send(r)
			time.Sleep(time.Second * 1)
		}
		_ = stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				close(waitCh)
				return
			}
			if err != nil {
				log.Fatal(err)
				return
			}
			fmt.Printf("Receiving: %s \n", res.GetResult())
		}

	}()

	<-waitCh
}

func doErrorUnary(c greet_pb.GreetServiceClient) {
	doErrorCall(c, 10)
	doErrorCall(c, -2)
}

func doErrorCall(c greet_pb.GreetServiceClient, n int32) {
	fmt.Println("Square root rpc start")
	input := &greet_pb.SquareRootRequest{
		Number: n,
	}

	res, err := c.SquareRoot(context.Background(), input)
	if err != nil {
		respErr, ok := status.FromError(err)
		if ok {
			fmt.Println(respErr.Message())
			fmt.Println(respErr.Code())
			if respErr.Code() == codes.InvalidArgument {
				fmt.Println("invalid argument!")
			}
		} else {
			log.Fatal(err)
		}
	}
	fmt.Println(res)
}

func doUnaryWithDeadline(c greet_pb.GreetServiceClient, s time.Duration) {
	fmt.Println("Start unary")
	req := &greet_pb.GreetWithDeadlineRequest{Greeting: &greet_pb.Greeting{
		FirstName: "lokard",
		LastName:  "deepmaker",
	},
	}
	ctx, cancel := context.WithTimeout(context.Background(), s)
	defer cancel()
	response, err := c.GreetWithDeadline(ctx, req)
	if err != nil {
		statusErr, ok := status.FromError(err)
		if ok {
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("deadline exceeded")
			} else {
				fmt.Println("unexpected error")
			}
		} else {
			log.Fatal(err)
		}
		return
	}

	fmt.Println(response)
}
