PROTO_DIR = proto
GEN_DIR   = proto/gen

.PHONY: proto build run docker-up docker-down clean

## Generate Go code from .proto definitions
proto:
	mkdir -p $(GEN_DIR)
	protoc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) $(PROTO_DIR)/cluster.proto

## Build the node binary
build:
	go build -o bin/node ./cmd/node

## Run a single node locally (useful for quick testing)
run:
	NODE_ID=node-a REST_ADDR=:8080 GRPC_ADDR=:9090 go run ./cmd/node

## Spin up the full 3-node cluster
docker-up:
	docker-compose up --build

## Tear down the cluster
docker-down:
	docker-compose down

clean:
	rm -rf bin/ proto/gen/
