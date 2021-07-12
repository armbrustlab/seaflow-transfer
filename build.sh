#!/bin/bash
# Build seaflow-transfer command-line tool for 64-bit MacOS and Linux

VERSION=$(git describe --tags)

[[ -d build ]] || mkdir build
GOOS=darwin GOARCH=amd64 go build -o build/seaflow-transfer.${VERSION}.darwin-amd64 cmd/seaflow-transfer/main.go || exit 1
GOOS=linux GOARCH=amd64 go build -o build/seaflow-transfer.${VERSION}.linux-amd64 cmd/seaflow-transfer/main.go || exit 1
openssl dgst -sha256 build/*.${VERSION}.* | sed -e 's|build/||g'
