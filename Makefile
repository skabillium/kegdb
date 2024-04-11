CMD = ./cmd
TEST = ./test
BIN = ./bin/keg

help:
	go run ${CMD} --help

install:
	go mod download

clean:
	rm -rf data
	rm -rf ./bin

tests:
	go test ${CMD}

dev:
	go run ${CMD}

build:
	go build -o ${BIN} ${CMD}

prod: clean build
	${BIN}
