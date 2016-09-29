ifdef GOBIN
	PATH := $(GOBIN):$(PATH)
else
	PATH := $(subst :,/bin:,$(GOPATH))/bin:$(PATH)
endif

GO   := GO15VENDOREXPERIMENT=1 go
pkgs  = $(shell $(GO) list ./... | grep -v /vendor/)

all: format build test

style:
	@echo ">> checking code style"
	@! gofmt -d $(shell find . -path ./vendor -prune -o -name '*.go' -print) | grep '^'

test:
	@echo ">> running tests"
	@$(GO) test -short $(pkgs)

format:
	@echo ">> formatting code"
	@$(GO) fmt $(pkgs)

vet:
	@echo ">> vetting code"
	@$(GO) vet $(pkgs)

build:
	@echo ">> building binaries"
	@$(GO) build -v -o ./bin/cloudinsight-agent

run: build
	./bin/cloudinsight-agent


.PHONY: all style format build test vet run
