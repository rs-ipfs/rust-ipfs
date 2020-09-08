#[cfg(feature = "test_go_interop")]
pub mod go {
    pub use super::common::*;
}

#[cfg(feature = "test_js_interop")]
pub mod js {
    pub use super::common::*;
}

#[cfg(all(not(feature = "test_go_interop"), not(feature = "test_js_interop")))]
pub mod none {
    pub struct ForeignNode;
}

#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
pub mod common {
    use anyhow;
    use libp2p::{core::PublicKey, Multiaddr, PeerId};
    use rand::prelude::*;
    use serde::Deserialize;
    use std::time::Duration;
    use std::{
        env, fs,
        path::PathBuf,
        process::{Child, Command, Stdio},
        thread,
    };

    // this environment variable should point to the location of the foreign ipfs binary
    #[cfg(feature = "test_go_interop")]
    pub const ENV_IPFS_PATH: &str = "GO_IPFS_PATH";
    #[cfg(feature = "test_js_interop")]
    pub const ENV_IPFS_PATH: &str = "JS_IPFS_PATH";

    pub struct ForeignNode {
        pub dir: PathBuf,
        pub daemon: Child,
        pub id: PeerId,
        pub pk: PublicKey,
        pub addrs: Vec<Multiaddr>,
    }

    impl ForeignNode {
        pub fn new() -> ForeignNode {
            let binary_path = env::vars()
                .find(|(key, _val)| key == ENV_IPFS_PATH)
                .unwrap_or_else(|| {
                    panic!("the {} environment variable was not found", ENV_IPFS_PATH)
                })
                .1;

            let mut tmp_dir = env::temp_dir();
            let mut rng = rand::thread_rng();
            tmp_dir.push(&format!("ipfs_test_{}", rng.gen::<u64>()));
            let _ = fs::create_dir(&tmp_dir);

            Command::new(&binary_path)
                .env("IPFS_PATH", &tmp_dir)
                .arg("init")
                .arg("-p")
                .arg("test")
                .arg("--bits")
                .arg("2048")
                .stdout(Stdio::null())
                .status()
                .unwrap();

            let daemon = Command::new(&binary_path)
                .env("IPFS_PATH", &tmp_dir)
                .arg("daemon")
                .stdout(Stdio::null())
                .spawn()
                .unwrap();

            // give the daemon a little bit of time to start
            thread::sleep(Duration::from_secs(1));

            let node_id = Command::new(&binary_path)
                .env("IPFS_PATH", &tmp_dir)
                .arg("id")
                .output()
                .unwrap()
                .stdout;

            let node_id_stdout = String::from_utf8_lossy(&node_id);
            let ForeignNodeId {
                id,
                addresses,
                public_key,
                ..
            } = serde_json::de::from_str(&node_id_stdout).unwrap();

            let id = id.parse().unwrap();
            let pk = PublicKey::from_protobuf_encoding(
                &base64::decode(public_key.into_bytes()).unwrap(),
            )
            .unwrap();

            ForeignNode {
                dir: tmp_dir,
                daemon,
                id,
                pk,
                addrs: addresses,
            }
        }

        #[allow(dead_code)]
        pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), anyhow::Error> {
            Ok((self.pk.clone(), self.addrs.clone()))
        }
    }

    impl Drop for ForeignNode {
        fn drop(&mut self) {
            let _ = self.daemon.kill();
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[derive(Deserialize, Debug)]
    #[cfg_attr(feature = "test_go_interop", serde(rename_all = "PascalCase"))]
    #[cfg_attr(feature = "test_js_interop", serde(rename_all = "camelCase"))]
    pub struct ForeignNodeId {
        #[cfg_attr(feature = "test_go_interop", serde(rename = "ID"))]
        pub id: String,
        pub public_key: String,
        pub addresses: Vec<Multiaddr>,
        #[serde(skip)]
        agent_version: String,
        #[serde(skip)]
        protocol_version: String,
    }
}
