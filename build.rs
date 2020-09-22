fn main() {
    prost_build::compile_protos(&["src/ipld/dag_pb.proto"], &["src"]).unwrap();
}
