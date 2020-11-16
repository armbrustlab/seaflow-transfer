#!/bin/bash
# Build seaflow-transfer command-line tool for 64-bit MacOS and Linux

[[ -e seaflow-transfer.darwin-amd64 ]] && rm seaflow-transfer.darwin-amd64
[[ -e seaflow-transfer.linux-amd64 ]] && rm seaflow-transfer.linux-amd64
GOOS=darwin GOARCH=amd64 go build -o seaflow-transfer.darwin-amd64 cmd/seaflow-transfer/main.go || exit 1
GOOS=linux GOARCH=amd64 go build -o seaflow-transfer.linux-amd64 cmd/seaflow-transfer/main.go || exit 1
openssl dgst -sha256 seaflow-transfer.darwin-amd64 >seaflow-transfer.darwin-amd64.sha256
openssl dgst -sha256 seaflow-transfer.linux-amd64 >seaflow-transfer.linux-amd64.sha256
