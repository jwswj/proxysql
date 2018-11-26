PROTO_DIR=../deps/protobuf/protobuf-3.6.1
PROTO_IDIR=$(PROTO_DIR)/src

PROTOC=$(PROTO_DIR)/src/protoc
MYCXXFLAGS=-std=c++11

mysql_event_v1.a: mysql_event_v1.pb.cc mysql_event_v1.pb.h
	$(CXX) $(MYCXXFLAGS) -fPIC -c -o mysql_event_v1.pb.o mysql_event_v1.pb.cc -I$(PROTO_IDIR)
	ar rvs mysql_event_v1.a mysql_event_v1.pb.o

mysql_event_v1.pb.cc mysql_event_v1.pb.h:
	${PROTOC} --cpp_out=. mysql_event_v1.proto

default: mysql_event_v1.a mysql_event_v1.pb.h

clean:
	rm -f mysql_event_v1.pb.cc mysql_event_v1.pb.h mysql_event_v1.pb.o mysql_event_v1.a
.PHONY: clean