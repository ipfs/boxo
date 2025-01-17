# This Makefile is designed to work around namespace conflicts that occur when
# multiple repositories depend on .proto files with generic filenames. Due to
# the way protobuf registers files (e.g., foo.proto or pb/foo.proto), naming
# collisions can occur when the same filename is used across different
# packages.
#
# The only way to generate a *.pb.go file that includes the full package path
# (e.g., github.com/org/repo/pb/foo.proto) is to place the .proto file in a
# directory structure that mirrors its package path.
#
# References:
# * https://protobuf.dev/reference/go/faq#namespace-conflict
# * https://github.com/golang/protobuf/issues/1122#issuecomment-2045945265

PKG_PATH = github.com/ipfs/boxo/ipns/pb
PROTONAME = record
PROTOFILE = ${PROTONAME}.proto

all: clean build

build:
	mkdir -p ${PKG_PATH}
	cp ${PROTOFILE} ${PKG_PATH} 
	protoc --go_out=. ${PKG_PATH}/${PROTOFILE}
	mv ${PKG_PATH}/${PROTONAME}.pb.go .
	rm -rf $(firstword $(subst /, ,${PKG_PATH}))

clean:
	rm -f *.pb.go

.PHONY: all build clean
