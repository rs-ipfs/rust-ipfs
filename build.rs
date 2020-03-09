fn main() {
    prost_build::compile_protos(&["src/bitswap/bitswap_pb.proto"], &["src"]).unwrap();
    prost_build::compile_protos(&["src/ipns/ipns_pb.proto"], &["src"]).unwrap();
}
