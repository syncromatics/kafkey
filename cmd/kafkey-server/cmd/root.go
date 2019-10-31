package cmd

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/syncromatics/kafkey/internal/kafkey"
	"google.golang.org/grpc"

	"github.com/spf13/cobra"
	"github.com/syncromatics/kafkey/internal/service"
)

var (
	broker   string
	registry string
	port     string
)

var rootCmd = &cobra.Command{
	Use:   "kafkey-server",
	Short: "Kafkey-server is the grpc part of the kafkey topic reader",
	Long:  `Kafkey-server is the grpc part of the kafkey topic reader"`,
	Run: func(cmd *cobra.Command, args []string) {
		run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&broker, "broker", "", "the broker")
	rootCmd.PersistentFlags().StringVar(&registry, "registry", "", "the avro schema registry")
	rootCmd.PersistentFlags().StringVar(&port, "port", "", "the port of the service")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() {
	if broker == "" {
		panic("broker is required")
	}

	if registry == "" {
		panic("registry is required")
	}

	if port == "" {
		panic("port is required")
	}

	service := service.New(broker, registry)

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	kafkey.RegisterKafkeyServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
