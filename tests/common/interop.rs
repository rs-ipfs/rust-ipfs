#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
pub use common::{api_call, ForeignNode};

#[cfg(all(not(feature = "test_go_interop"), not(feature = "test_js_interop")))]
#[allow(dead_code)]
pub struct ForeignNode;

#[cfg(any(feature = "test_go_interop", feature = "test_js_interop"))]
pub mod common {
    use libp2p::{core::PublicKey, Multiaddr, PeerId};
    use rand::prelude::*;
    use serde::Deserialize;
    use std::{
        env, fs,
        path::PathBuf,
        process::{Child, Command, Stdio},
    };

    #[derive(Debug)]
    pub struct ForeignNode {
        pub dir: PathBuf,
        pub daemon: Child,
        pub id: PeerId,
        pub pk: PublicKey,
        pub addrs: Vec<Multiaddr>,
        pub binary_path: String,
        pub api_port: u16,
    }

    impl ForeignNode {
        #[allow(dead_code)]
        pub fn new() -> ForeignNode {
            use std::{io::Read, net::SocketAddr, str};

            // this environment variable should point to the location of the foreign ipfs binary
            #[cfg(feature = "test_go_interop")]
            const ENV_IPFS_PATH: &str = "GO_IPFS_PATH";
            #[cfg(feature = "test_js_interop")]
            const ENV_IPFS_PATH: &str = "JS_IPFS_PATH";

            // obtain the path of the foreign ipfs binary from an environment variable
            let binary_path = env::vars()
                .find(|(key, _val)| key == ENV_IPFS_PATH)
                .unwrap_or_else(|| {
                    panic!("the {} environment variable was not found", ENV_IPFS_PATH)
                })
                .1;

            // create the temporary directory for the repo etc
            let mut tmp_dir = env::temp_dir();
            let mut rng = rand::thread_rng();
            tmp_dir.push(&format!("ipfs_test_{}", rng.gen::<u64>()));
            let _ = fs::create_dir(&tmp_dir);

            // initialize the node and assign the temporary directory to it
            Command::new(&binary_path)
                .env("IPFS_PATH", &tmp_dir)
                .arg("init")
                .arg("-p")
                .arg("test")
                .stdout(Stdio::null())
                .status()
                .unwrap();

            #[cfg(feature = "test_go_interop")]
            let daemon_args = &["daemon", "--enable-pubsub-experiment"];
            #[cfg(feature = "test_js_interop")]
            let daemon_args = &["daemon"];

            // start the ipfs daemon
            let mut daemon = Command::new(&binary_path)
                .env("IPFS_PATH", &tmp_dir)
                .args(daemon_args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();

            // read the stdout of the spawned daemon...
            let mut buf = vec![0; 2048];
            let mut index = 0;
            if let Some(ref mut stdout) = daemon.stdout {
                while let Ok(read) = stdout.read(&mut buf[index..]) {
                    index += read;
                    if str::from_utf8(&buf).unwrap().contains("Daemon is ready") {
                        break;
                    }
                }
            }

            // ...so that the randomly assigned API port can be registered
            let mut api_port = None;
            for line in str::from_utf8(&buf).unwrap().lines() {
                if line.contains("webui") {
                    let addr = line.rsplitn(2, ' ').next().unwrap();
                    let addr = addr.strip_prefix("http://").unwrap();
                    let addr: SocketAddr = addr.rsplitn(2, '/').nth(1).unwrap().parse().unwrap();
                    api_port = Some(addr.port());
                }
            }
            let api_port = api_port.unwrap();

            // run /id in order to register the PeerId, PublicKey and Multiaddrs assigned to the node
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
                binary_path,
                api_port,
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

    // this one is not a method on ForeignNode, as only its port number is needed and we don't
    // want to restrict ourselves from calling it from spawned tasks or threads (or to make the
    // internals of ForeignNode complicated by making it Clone)
    #[allow(dead_code)]
    pub async fn api_call<T: AsRef<str>>(api_port: u16, call: T) -> String {
        let bytes = Command::new("curl")
            .arg("-X")
            .arg("POST")
            .arg(&format!(
                "http://127.0.0.1:{}/api/v0/{}",
                api_port,
                call.as_ref()
            ))
            .output()
            .unwrap()
            .stdout;
        String::from_utf8(bytes).unwrap()
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
