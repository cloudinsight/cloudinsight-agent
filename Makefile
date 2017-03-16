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
	@$(GO) test -v -cover=true -short $(pkgs)

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

generate-cover-data:
	@echo ">> generating coverage profiles"
	@echo "mode: count" > coverage-all.out
	@$(foreach pkg,$(pkgs),\
		go test -v -coverprofile=coverage.out -covermode=count $(pkg) || exit $$?;\
		if [ -f coverage.out ]; then\
		    tail -n +2 coverage.out >> coverage-all.out;\
                fi\
		;)

test-cover-html: generate-cover-data
	@$(GO) tool cover -html=coverage-all.out -o coverage.html

test-cover-func: generate-cover-data
	@$(GO) tool cover -func=coverage-all.out


.PHONY: all style format build test vet run test-cover-html test-cover-func generate-cover-data
