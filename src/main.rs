use std::{fs, sync::Arc};

use agent::Agent;
use lazy_static::lazy_static;
use metadata::MetadataStore;
use std::sync::Mutex;
use types::{ReadRequest, WriteRequest};

mod agent;
mod metadata;
mod types;

lazy_static! {
    static ref METADATA_STORE: Arc<Mutex<MetadataStore>> =
        Arc::new(Mutex::new(MetadataStore::new()));
}

fn main() {
    let _ = (fs::remove_dir_all("s3"), fs::create_dir("s3"));

    let mut agent = Agent::new(1);

    agent.write(WriteRequest {
        topic: "1".to_owned(),
        partition: "1".to_owned(),
        value: String::from("test").into_bytes(),
    });
    agent.write(WriteRequest {
        topic: "1".to_owned(),
        partition: "1".to_owned(),
        value: String::from("another").into_bytes(),
    });
    agent.flush();

    agent.write(WriteRequest {
        topic: "2".to_owned(),
        partition: "2".to_owned(),
        value: String::from("test2").into_bytes(),
    });
    agent.write(WriteRequest {
        topic: "1".to_owned(),
        partition: "1".to_owned(),
        value: String::from("follow").into_bytes(),
    });
    agent.flush();

    let request = ReadRequest {
        topic: "1".to_owned(),
        partition: "1".to_owned(),
        offsets: (3, 3),
    };
    let result = agent.read(request).unwrap();

    result
        .values
        .into_iter()
        .for_each(|value| print!("{} ", String::from_utf8(value).unwrap()));

    println!();
    let request = ReadRequest {
        topic: "2".to_owned(),
        partition: "2".to_owned(),
        offsets: (1, 1),
    };
    let result = agent.read(request).unwrap();
    println!("{}", String::from_utf8(result.values[0].clone()).unwrap());
}

pub fn get_metadata_store() -> Arc<Mutex<MetadataStore>> {
    METADATA_STORE.clone()
}
