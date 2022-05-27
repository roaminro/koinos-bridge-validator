protoc --experimental_allow_proto3_optional \
   --go_out=proto/build/ \
   `find proto -name '*.proto'`