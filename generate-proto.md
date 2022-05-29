protoc --experimental_allow_proto3_optional \
   --descriptor_set_out=proto/build/bridge_descriptors.pb \
   --go_out=proto/build/ \
   `find proto -name '*.proto'`