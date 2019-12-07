#!/bin/bash
# Build seaflow-transfer command-line tool for 64-bit MacOS and Linux

[[ -d seaflow-transfer.darwin-amd64 ]] && rm -rf seaflow-transfer.darwin-amd64
[[ -d seaflow-transfer.linux-amd64 ]] && rm -rf seaflow-transfer.linux-amd64
GOOS=darwin GOARCH=amd64 go build -o seaflow-transfer.darwin-amd64/seaflow-transfer cmd/seaflow-transfer/main.go || exit 1
GOOS=linux GOARCH=amd64 go build -o seaflow-transfer.linux-amd64/seaflow-transfer cmd/seaflow-transfer/main.go || exit 1
zip -q -r seaflow-transfer.darwin-amd64.zip seaflow-transfer.darwin-amd64 || exit 1
zip -q -r seaflow-transfer.linux-amd64.zip seaflow-transfer.linux-amd64 || exit 1
