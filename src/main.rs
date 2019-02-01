use ipfs::p2p::{run_service, Service};

fn main() {
    let peer_id = "QmdiXyMWRbsP8681LjnJG2Qz7maMpomTMaKQmqEy7Ato9x";
    let mut service = Service::new();
    service.find_peer(peer_id);
    println!("Looking for peer_id: {}", peer_id);
    run_service(service);
}
