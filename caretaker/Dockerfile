FROM golang:alpine
LABEL maintainer Child of the Universe <childoftheuniverse@protonmail.com>

# Set up protocol buffer compile pipeline
RUN apk --update add git protobuf
RUN go get github.com/golang/protobuf/protoc-gen-go

# Compile the protocol buffers in all dependencies
RUN go get -d github.com/childoftheuniverse/etcd-discovery
RUN cd /go/src/github.com/childoftheuniverse/etcd-discovery; protoc --go_out=plugins=grpc:. *.proto

# Now, get and install the main binary
RUN go get -d github.com/childoftheuniverse/red-cloud/caretaker
RUN cd /go/src/github.com/childoftheuniverse/red-cloud; protoc --go_out=plugins=grpc:. *.proto
RUN go get github.com/childoftheuniverse/red-cloud/caretaker

RUN ["/usr/sbin/adduser", "-S", "-h", "/nonexistent", "-H", "-g", "Red Cloud Caretaker", "-g", "_caretaker", "-D", "_caretaker"]
VOLUME ["/secrets"]
USER _caretaker
ENTRYPOINT ["/go/bin/caretaker"]
