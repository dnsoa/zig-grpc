#!/bin/sh
# One-shot interop run: build+start the grpc-go echo server, run the zig
# client against it, tear down. Requires a local Go toolchain.
set -e
cd "$(dirname "$0")/.."

mkdir -p zig-out
(cd testdata/go-server && go build -o "$OLDPWD/zig-out/go-echo-server" .)
zig build interop

zig-out/go-echo-server &
GO_PID=$!
trap 'kill $GO_PID 2>/dev/null' EXIT
sleep 1

./zig-out/bin/zig-grpc-interop
echo "INTEROP OK"
