use lightning::{aarc::Arc, map::PtrHashMap};

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::thread;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
    id: u64,
    stage_id: u64,
    data_partition: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct Stage {
    id: u64,
    tasks: Vec<Task>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: u64,
    stages: Vec<Stage>,
}

struct Coordinator {
    jobs: PtrHashMap<u64, Arc<Job>>,
}
