use crate::agent::OffsetRange;
use crate::{agent::ReadRequest, BatchMetadata, TopicPartition};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::Included;

pub type RecordOffsetAndMetadata = BTreeMap<usize, BatchMetadata>;
pub type BatchReads = (OffsetRange, Vec<BatchRead>);

#[derive(Debug)]
pub struct BatchRead {
    pub file_name: String,
    pub file_offset: usize,
    pub length: usize,
}

pub struct MetadataStore {
    metadata: TopicPartition<RecordOffsetAndMetadata>,
}

impl MetadataStore {
    pub fn new() -> Self {
        MetadataStore {
            metadata: HashMap::new(),
        }
    }

    pub fn write(&mut self, received_metadata: TopicPartition<BatchMetadata>, agent_id: usize) {
        println!("Receiving write request from agent {}", agent_id);
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
                    max_key.0 + max_key.1.record_sizes.len() + 1
                } else {
                    1
                };

                offsets_to_metadata.insert(next_offset, batch_metadata);
            }
        }
    }

    pub fn read(&self, request: ReadRequest, agent_id: usize) -> Result<BatchReads, ()> {
        println!("Receiving read request from agent {}", agent_id);
        let ReadRequest {
            topic,
            partition,
            offsets,
        } = request;

        if let Some(offsets_to_metadata) = self
            .metadata
            .get(&topic)
            .and_then(|partitions| partitions.get(&partition))
        {
            println!("{:?}", offsets_to_metadata);
            let (start, end) = offsets;
            let offset_keys: Vec<usize> = offsets_to_metadata
                .range((Included(start), Included(end)))
                .map(|(&key, _)| key)
                .collect();

            let reads = offset_keys
                .iter()
                .enumerate()
                .map(|(i, offset)| {
                    let batch_metadata = &offsets_to_metadata[offset];

                    let length = if i == offset_keys.len() - 1 {
                        batch_metadata.record_sizes.iter().take(end - offset).sum()
                    } else {
                        batch_metadata.record_sizes.iter().sum()
                    };

                    BatchRead {
                        file_name: batch_metadata.file_name.clone(),
                        file_offset: batch_metadata.batch_offset,
                        length,
                    }
                })
                .collect::<Vec<BatchRead>>();

            Ok((offsets, reads))
        } else {
            Err(())
        }
    }
}
