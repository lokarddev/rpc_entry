package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
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
	doBiDiStreaming(c)
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
