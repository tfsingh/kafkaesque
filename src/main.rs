use std::time::Duration;
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

    let shared_agent = Arc::new(Mutex::new(Agent::new(1)));
    {
        let agent = Arc::clone(&shared_agent);
        std::thread::spawn(move || {
            let mut agent = agent.lock().unwrap();
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
        });
    }

    let agent = Arc::clone(&shared_agent);
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        let agent = agent.lock().unwrap();
        let request = ReadRequest {
            topic: "1".to_owned(),
            partition: "1".to_owned(),
            offsets: (1, 3),
        };
        let result = agent.read(request).unwrap();
        //assert!(String::from_utf8(result.values[0].clone()).unwrap() == "follow");
        result
            .values
            .into_iter()
            .for_each(|value| println!("{}", String::from_utf8(value).unwrap()));

        let request = ReadRequest {
            topic: "2".to_owned(),
            partition: "2".to_owned(),
            offsets: (1, 2),
        };
        let result = agent.read(request).unwrap();
        assert!(String::from_utf8(result.values[0].clone()).unwrap() == "test2");
    });

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

pub fn get_metadata_store() -> Arc<Mutex<MetadataStore>> {
    METADATA_STORE.clone()
}
