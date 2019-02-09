FROM golang:1.10
LABEL maintainer Child of the Universe <childoftheuniverse@protonmail.com>

# Set up protocol buffer compile pipeline
RUN apt-get -q -y update && apt-get -q -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold --purge install gcc libc-dev protobuf-compiler librados-dev
RUN go get github.com/golang/protobuf/protoc-gen-go

# etcd fix
RUN go get go.etcd.io/etcd
RUN grep -rl github.com/coreos/etcd /go/src/go.etcd.io/etcd | xargs sed -i -e's@github.com/coreos/etcd@go.etcd.io/etcd@g'
RUN rm -fr /go/src/go.etcd.io/etcd/vendor/golang.org/x/net/trace

# Compile the protocol buffers in all dependencies
RUN go get -d github.com/childoftheuniverse/etcd-discovery
RUN cd /go/src/github.com/childoftheuniverse/etcd-discovery; protoc --go_out=plugins=grpc:. *.proto

# Now, get and install the main binary
RUN go get -d github.com/childoftheuniverse/red-cloud/caretaker
RUN cd /go/src/github.com/childoftheuniverse/red-cloud; protoc --go_out=plugins=grpc:. *.proto
RUN go get github.com/childoftheuniverse/red-cloud/caretaker

RUN ["/usr/sbin/adduser", "--system", "--home", "/nonexistent", "--no-create-home", "--gecos", "Red Cloud Caretaker", "--group", "--disabled-password", "caretaker"]
VOLUME ["/tls", "/ca"]
USER caretaker
ENTRYPOINT ["/go/bin/caretaker"]
