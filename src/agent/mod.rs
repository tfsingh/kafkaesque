use std::collections::HashMap;
use tokio::{
    io::AsyncWriteExt,
    time::{sleep, Duration},
};
use uuid::Uuid;

use crate::{BatchMetadata, TopicPartition};

#[derive(Default)]
pub struct AgentWriteRequest {
    topic: String,
    partition: String,
    value: Vec<u8>,
}

#[derive(Default)]
pub struct Agent {
    // Topic -> Partition -> Vec<values>
    active_records: TopicPartition<Vec<u8>>,
    // Topic -> Partition -> Record sizes (in bytes)
    active_metadata: TopicPartition<Vec<usize>>,
}

impl Agent {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    #[tokio::main]
    pub async fn flush(&mut self) {
        sleep(Duration::from_millis(250)).await;

        // Topic -> Partition -> (Batch offset in the file, sizes of records)
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

        // Write to file
        tokio::spawn(async move {
            let mut file = match tokio::fs::File::create(file_name).await {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("Error creating file: {}", e);
                    return;
                }
            };

            if let Err(e) = file.write_all(&file_bytes).await {
                eprintln!("Error writing to file: {}", e);
                return;
            }
        });

        self.flush();
    }

    pub fn write(&mut self, request: AgentWriteRequest) {
        let AgentWriteRequest {
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
}
