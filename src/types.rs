use std::collections::{BTreeMap, HashMap};

/// ------- Global -------

// Used to define a hashmap of topic -> partition -> T
pub type TopicPartition<T> = HashMap<String, HashMap<String, T>>;

/// ------- Write flow -------

// Sorted map of starting offset -> metadata for a batch
pub type RecordOffsetAndMetadata = BTreeMap<usize, BatchMetadata>;

// Stores the file name, byte offset, and sizes of each record
// in bytes, agent -> metadata store on flush
#[derive(Debug)]
pub struct BatchMetadata {
    pub file_name: String,
    pub file_offset: usize,
    pub record_sizes: Vec<usize>,
}

// Producer -> agent
pub struct WriteRequest {
    pub topic: String,
    pub partition: String,
    pub value: Vec<u8>,
}

/// ------- Read flow -------

// Range of offsets sent by a consumer to read
pub type OffsetRange = (usize, usize);
// Returned by the metadata store to indicate a reads for an offset range
pub type BatchReads = (OffsetRange, Vec<BatchRead>);

// File name, byte offset, and sizes of records to read
#[derive(Debug)]
pub struct BatchRead {
    pub file_name: String,
    pub file_offset: usize,
    pub record_sizes: Vec<usize>,
}

// Consumer -> agent
#[derive(Default)]
pub struct ReadRequest {
    pub topic: String,
    pub partition: String,
    pub offsets: OffsetRange,
}

// Agent -> consumer
#[derive(Default)]
pub struct ReadResult {
    pub offsets: OffsetRange,
    pub values: Vec<Vec<u8>>,
}
