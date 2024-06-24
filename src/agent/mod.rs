use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
};
use uuid::Uuid;

use crate::{get_metadata_store, metadata::BatchRead, BatchMetadata, TopicPartition};

pub type OffsetRange = (usize, usize);

#[derive(Default)]
pub struct WriteRequest {
    pub topic: String,
    pub partition: String,
    pub value: Vec<u8>,
}

#[derive(Default)]
pub struct ReadRequest {
    pub topic: String,
    pub partition: String,
    pub offsets: OffsetRange,
}

#[derive(Default)]
pub struct ReadResult {
    pub offsets: OffsetRange,
    pub values: Vec<Vec<u8>>,
}

#[derive(Default)]
pub struct Agent {
    active_records: TopicPartition<Vec<u8>>,
    active_metadata: TopicPartition<Vec<usize>>,
    id: usize,
}

impl Agent {
    pub fn new(num: usize) -> Self {
        Self {
            id: num,
            ..Default::default()
        }
    }

    pub fn flush(&mut self) {
        let mut metadata: TopicPartition<BatchMetadata> = HashMap::new();
        let mut file_bytes: Vec<u8> = Vec::new();
        let mut batch_offset = 0;

        let uuid = Uuid::new_v4();
        let file_name = format!("{}.bin", uuid.to_hyphenated());

        for (topic, partitions) in &self.active_records {
            for (partition, data) in partitions {
                file_bytes.extend(data);

                let partition_metadata = metadata.entry(topic.clone()).or_insert_with(HashMap::new);
                let record_sizes = self.active_metadata[topic][partition].clone();
                let batch_size: usize = record_sizes.iter().sum();
                partition_metadata.insert(
                    partition.clone(),
                    BatchMetadata {
                        file_name: file_name.clone(),
                        batch_offset,
                        record_sizes,
                    },
                );

                batch_offset += batch_size;
            }
        }

        self.flush_and_send_metadata(file_name, file_bytes, metadata, self.id);

        self.active_records.clear();
        self.active_metadata.clear();
    }

    fn flush_and_send_metadata(
        &self,
        file_name: String,
        file_bytes: Vec<u8>,
        metadata: TopicPartition<BatchMetadata>,
        id: usize,
    ) {
        let full_file_path = format!("s3/{}", file_name);
        let mut file = match File::create(&full_file_path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Error creating file: {}", e);
                return;
            }
        };

        if let Err(e) = file.write_all(&file_bytes) {
            eprintln!("Error writing to file: {}", e);
            return;
        }

        let metadata_store = get_metadata_store();
        let mut lock = metadata_store.lock().unwrap();
        lock.write(metadata, id);
    }

    pub fn write(&mut self, request: WriteRequest) {
        let WriteRequest {
            topic,
            partition,
            value,
        } = request;

        let topic_records = self
            .active_records
            .entry(topic.clone())
            .or_insert_with(HashMap::new);
        let topic_metadata = self
            .active_metadata
            .entry(topic.clone())
            .or_insert_with(HashMap::new);

        if let Some(partition_data) = topic_records.get_mut(&partition) {
            partition_data.extend(&value);
        } else {
            topic_records.insert(partition.clone(), value.clone());
        }

        if let Some(partition_metadata) = topic_metadata.get_mut(&partition) {
            partition_metadata.push(value.len());
        } else {
            topic_metadata.insert(partition.clone(), vec![value.len()]);
        }
    }

    pub fn read(&self, request: ReadRequest) -> Result<ReadResult, ()> {
        let id = self.id;

        let metadata_store = get_metadata_store();
        let lock = metadata_store.lock().unwrap();
        let reads = lock.read(request, id)?;

        println!("{:?}", reads);

        let results = reads
            .1
            .iter()
            .map(|read| {
                let BatchRead {
                    file_name,
                    file_offset,
                    length,
                } = read;

                let file_path = format!("s3/{}", file_name);

                let mut file = File::open(&file_path).unwrap();
                file.seek(std::io::SeekFrom::Start(*file_offset as u64))
                    .unwrap();

                let mut buffer = vec![0; *length as usize];
                file.read_exact(&mut buffer).unwrap();

                buffer
            })
            .collect();

        Ok(ReadResult {
            offsets: reads.0,
            values: results,
        })
    }
}
