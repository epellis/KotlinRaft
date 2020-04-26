#!/usr/bin/env bash

SOURCE_DIR="../src/main/proto"
DEST_DIR="./src"

rm ./src/*_pb.js

protoc -I=$SOURCE_DIR control.proto \
--js_out=import_style=commonjs:$DEST_DIR \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:$DEST_DIR
