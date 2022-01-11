PROG=lancelot
REPO_PATH=gitlab.s.upyun.com/platform/$(PROG)
GIT_COMMIT=$(shell git rev-parse --short HEAD)
GIT_DESCRIBE=$(shell git describe --tags --always)
IMPORT=$(REPO_PATH)/version
GOLDFLAGS=-X $(IMPORT).GitCommit=$(GIT_COMMIT) -X $(IMPORT).GitDescribe=$(GIT_DESCRIBE)

WORK_DIR=$(shell pwd)

ifeq ($(shell uname -s), Darwin)
PLAT=osx
SED_EXTENSION=""
else
PLAT=linux
endif

debug:
	go build -gcflags=all="-N -l" -o bin/lancelot ./cmd/lancelot

app:
	go-bindata -o command/commands.go -pkg=command asset
	CGO_ENABLED=0 go build -ldflags '$(GOLDFLAGS)' -o bin/lancelot ./cmd/lancelot

lint:
	revive -config ./revive.toml -formatter friendly ./...

start:
	./bin/lancelot -dev

test: lint
	go test ./...
	nohup ./bin/lancelot -dev &
	sleep 3
	echo ${SHELL}
	./redistest.sh

.PHONY: test lint app
