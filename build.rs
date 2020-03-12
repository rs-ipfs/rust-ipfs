fn main() {
    prost_build::compile_protos(&["src/ipns/ipns_pb.proto"], &["src"]).unwrap();
}
