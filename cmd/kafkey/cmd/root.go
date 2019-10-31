package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/syncromatics/kafkey/internal/kafkey"

	"google.golang.org/grpc"

	"github.com/spf13/cobra"
)

var (
	service string
	topic   string
	key     string
)

var rootCmd = &cobra.Command{
	Use:   "kafkey",
	Short: "Kafkey is a kafka topic reader",
	Long:  `Kafkey is a kafka topic reader`,
	Run: func(cmd *cobra.Command, args []string) {
		run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&service, "service", "", "the kafkey-service")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "the topic to watch")
	rootCmd.PersistentFlags().StringVar(&key, "key", "", "the key to filter")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() {
	conn, err := grpc.Dial(service, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := kafkey.NewKafkeyClient(conn)

	stream, err := client.Watch(context.Background(), &kafkey.WatchRequest{
		Topic:  topic,
		Key:    key,
		Offset: kafkey.Offset_EARLIEST,
	})
	if err != nil {
		panic(err)
	}
	defer stream.CloseSend()

	for {
		r, err := stream.Recv()
		if err != nil {
			panic(err)
		}

		fmt.Println(r.Message)
	}
}
