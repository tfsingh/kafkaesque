use std::collections::HashMap;

mod agent;
mod metadata;

type TopicPartition<T> = HashMap<String, HashMap<String, T>>;

pub struct BatchMetadata {
    file_name: String,
    batch_offset: usize,
    record_sizes: Vec<usize>,
}

pub fn main() {}
