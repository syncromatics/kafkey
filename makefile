build:
	go build -o=artifacts/kafkey-server ./cmd/kafkey-server/main.go
	go build -o=artifacts/kafkey ./cmd/kafkey/main.go

generate-proto:
	mkdir -p internal/kafkey
	protoc --proto_path=docs/protos --go_out=plugins=grpc:internal/kafkey/ docs/protos/*.proto 
