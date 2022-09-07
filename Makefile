# Set V to 1 for verbose output from the Makefile
Q=$(if $V,,@)
PREFIX?=
SRC=$(shell find . -type f -name '*.go' -not -path "./vendor/*")
GOOS_OVERRIDE ?=
OUTPUT_ROOT=output/

all: test lint

ci: ci-test

.PHONY: all

#########################################
# Build
#########################################

build: ;

#########################################
# Bootstrapping
#########################################

bootstra%:
	# Using a released version of golangci-lint to take into account custom replacements in their go.mod
	$Q curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.49
	$Q go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: bootstra%

#########################################
# Test
#########################################
test:
	$Q $(GOFLAGS) go test -short -coverprofile=coverage.out ./...

ci-test:
	$Q $(GOFLAGS) CI=1 go test -short -coverprofile=coverage.out ./...

.PHONY: test ci-test

#########################################
# Linting
#########################################

fmt:
	$Q goimports -local github.com/golangci/golangci-lint -l -w $(SRC)

lint: SHELL:=/bin/bash
lint:
	$Q LOG_LEVEL=error golangci-lint run --config <(curl -s https://raw.githubusercontent.com/smallstep/workflows/master/.golangci.yml) --timeout=30m
	$Q govulncheck ./...

.PHONY: fmt lint

#########################################
# Clean
#########################################

clean:
ifneq ($(BINNAME),"")
	$Q rm -f bin/$(BINNAME)
endif

.PHONY: clean
