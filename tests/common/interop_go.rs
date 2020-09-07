#[cfg(feature = "test_dht_with_go")]
use rand::prelude::*;
#[cfg(feature = "test_dht_with_go")]
use serde::Deserialize;
#[cfg(feature = "test_dht_with_go")]
use std::time::Duration;
#[cfg(feature = "test_dht_with_go")]
use std::{
    env, fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
};

#[cfg(feature = "test_dht_with_go")]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct GoNodeId {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(skip)]
    pub public_key: String,
    pub addresses: Vec<String>,
    #[serde(skip)]
    agent_version: String,
    #[serde(skip)]
    protocol_version: String,
}

#[cfg(feature = "test_dht_with_go")]
pub struct GoIpfsNode {
    daemon: Child,
    pub id: GoNodeId,
    dir: PathBuf,
}

impl GoIpfsNode {
    #[cfg(feature = "test_dht_with_go")]
    pub fn new() -> GoIpfsNode {
        // GO_IPFS_PATH should point to the location of the go-ipfs binary
        let go_ipfs_path = env::vars()
            .find(|(key, _val)| key == "GO_IPFS_PATH")
            .expect("the GO_IPFS_PATH environment variable was not found")
            .1;

        let mut tmp_dir = env::temp_dir();
        let mut rng = rand::thread_rng();
        tmp_dir.push(&format!("ipfs_test_{}", rng.gen::<u64>()));
        let _ = fs::create_dir(&tmp_dir);

        Command::new(&go_ipfs_path)
            .env("IPFS_PATH", &tmp_dir)
            .arg("init")
            .arg("-p")
            .arg("test")
            .arg("--bits")
            .arg("2048")
            .stdout(Stdio::null())
            .status()
            .unwrap();

        let go_daemon = Command::new(&go_ipfs_path)
            .env("IPFS_PATH", &tmp_dir)
            .arg("daemon")
            .stdout(Stdio::null())
            .spawn()
            .unwrap();

        // give the go-ipfs daemon a little bit of time to start
        thread::sleep(Duration::from_secs(1));

        let go_id = Command::new(&go_ipfs_path)
            .env("IPFS_PATH", &tmp_dir)
            .arg("id")
            .output()
            .unwrap()
            .stdout;

        let go_id_stdout = String::from_utf8_lossy(&go_id);
        let go_id: GoNodeId = serde_json::de::from_str(&go_id_stdout).unwrap();

        GoIpfsNode {
            daemon: go_daemon,
            id: go_id,
            dir: tmp_dir,
        }
    }
}

#[cfg(not(feature = "test_dht_with_go"))]
pub struct GoIpfsNode;

#[cfg(feature = "test_dht_with_go")]
impl Drop for GoIpfsNode {
    fn drop(&mut self) {
        let _ = self.daemon.kill();
        let _ = fs::remove_dir_all(&self.dir);
    }
}
