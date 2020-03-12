fn main() {
    prost_build::compile_protos(&["src/bitswap_pb.proto"], &["src"]).unwrap();
}
