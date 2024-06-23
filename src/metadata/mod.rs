use crate::{BatchMetadata, TopicPartition};
use std::collections::{BTreeMap, HashMap};

pub type RecordOffsetAndMetadata = BTreeMap<usize, BatchMetadata>;

pub struct Metadata {
    metadata: TopicPartition<RecordOffsetAndMetadata>,
}

impl Metadata {
    pub fn new() -> Self {
        Metadata {
            metadata: HashMap::new(),
        }
    }

    pub async fn write(&mut self, received_metadata: TopicPartition<BatchMetadata>) {
        for (topic, partitions) in received_metadata {
            for (partition, batch_metadata) in partitions {
                let partition_metadata = self
                    .metadata
                    .entry(topic.clone())
                    .or_insert_with(HashMap::new);

                let offsets_to_metadata = partition_metadata
                    .entry(partition.clone())
                    .or_insert_with(BTreeMap::new);

                let next_offset = if let Some(max_key) = offsets_to_metadata.last_key_value() {
                    max_key.0 + max_key.1.record_sizes.len()
                } else {
                    0
                };

                offsets_to_metadata.insert(next_offset, batch_metadata);
            }
        }
    }
}
