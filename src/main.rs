use std::time::Duration;
use std::{collections::HashMap, fs, sync::Arc};

use agent::{Agent, ReadRequest, WriteRequest};
use lazy_static::lazy_static;
use metadata::MetadataStore;
use std::sync::Mutex;

mod agent;
mod metadata;

type TopicPartition<T> = HashMap<String, HashMap<String, T>>;

lazy_static! {
    static ref METADATA_STORE: Arc<Mutex<MetadataStore>> =
        Arc::new(Mutex::new(MetadataStore::new()));
}

#[derive(Debug)]
pub struct BatchMetadata {
    file_name: String,
    batch_offset: usize,
    record_sizes: Vec<usize>,
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
            agent.flush();
            agent.write(WriteRequest {
                topic: "2".to_owned(),
                partition: "2".to_owned(),
                value: String::from("test2").into_bytes(),
            });
            agent.flush();
        });
    }

    std::thread::spawn(move || {
        let agent = Arc::clone(&shared_agent);
        std::thread::sleep(std::time::Duration::from_millis(500));
        let agent = agent.lock().unwrap();
        let request = ReadRequest {
            topic: "1".to_owned(),
            partition: "1".to_owned(),
            offsets: (1, 2),
        };
        let result = agent.read(request).unwrap();
        println!(
            "Read result: {:?}",
            String::from_utf8(result.values[0].clone())
        );

        let request = ReadRequest {
            topic: "2".to_owned(),
            partition: "2".to_owned(),
            offsets: (1, 2),
        };
        let result = agent.read(request).unwrap();
        println!(
            "Read result: {:?}",
            String::from_utf8(result.values[0].clone())
        );
    });

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

pub fn get_metadata_store() -> Arc<Mutex<MetadataStore>> {
    METADATA_STORE.clone()
}
