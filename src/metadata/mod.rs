use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Included, Unbounded};

use crate::types::{
    BatchMetadata, BatchRead, BatchReads, ReadRequest, RecordOffsetAndMetadata, TopicPartition,
};

pub struct MetadataStore {
    metadata: TopicPartition<RecordOffsetAndMetadata>,
}

impl MetadataStore {
    pub fn new() -> Self {
        MetadataStore {
            metadata: HashMap::new(),
        }
    }

    pub fn write(&mut self, received_metadata: TopicPartition<BatchMetadata>, _agent_id: usize) {
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
                    1
                };

                offsets_to_metadata.insert(next_offset, batch_metadata);
            }
        }
    }

    pub fn read(&self, request: ReadRequest, _agent_id: usize) -> Result<BatchReads, ()> {
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
            let (start, end) = offsets;

            let nearest_smaller_or_equal_key = |offset| {
                offsets_to_metadata
                    .range((Unbounded, Included(offset)))
                    .next_back()
                    .map_or(offset, |(&key, _)| key)
            };

            let (start_key, end_key) = (
                nearest_smaller_or_equal_key(start),
                nearest_smaller_or_equal_key(end),
            );

            let offset_keys: Vec<usize> = offsets_to_metadata
                .range((Included(start_key), Included(end_key)))
                .map(|(&key, _)| key)
                .collect();

            let reads = offset_keys
                .iter()
                .enumerate()
                .map(|(i, offset)| {
                    let batch_metadata = &offsets_to_metadata[offset];
                    let mut file_offset = batch_metadata.file_offset;

                    let length = if i == 0 {
                        file_offset = batch_metadata
                            .record_sizes
                            .iter()
                            .take(start - offset)
                            .sum();

                        batch_metadata
                            .record_sizes
                            .iter()
                            .skip(start - offset)
                            .take(end - start + 1)
                            .sum()
                    } else if i == offset_keys.len() - 1 {
                        batch_metadata
                            .record_sizes
                            .iter()
                            .take(end - offset + 1)
                            .sum()
                    } else {
                        batch_metadata.record_sizes.iter().sum()
                    };

                    BatchRead {
                        file_name: batch_metadata.file_name.clone(),
                        file_offset,
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
