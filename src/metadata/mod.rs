use crate::types::{
    BatchMetadata, BatchRead, BatchReads, ReadRequest, RecordOffsetAndMetadata, TopicPartition,
};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::Included;

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
                        batch_metadata
                            .record_sizes
                            .iter()
                            .take(end - offset + 1)
                            .sum()
                    } else {
                        batch_metadata.record_sizes.iter().sum()
                    };

                    let file_offset = batch_metadata.file_offset;

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
