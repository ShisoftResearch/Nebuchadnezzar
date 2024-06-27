use itertools::Itertools;
use lightning::map::Map;
use lightning::{aarc::Arc, map::PtrHashMap};

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use crate::exec::dag::{Thread, DAG};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
    id: u64,
    stage_id: u64,
    data_partition: u64,
    dag_thread: Thread,
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
    dag: DAG,
}

struct Coordinator {
    job_id_cnt: AtomicU64,
    jobs: PtrHashMap<u64, Arc<Job>>,
}

impl Coordinator {
    pub fn new_job(&self, dag: DAG) -> u64 {
        let job_id = self.job_id_cnt.fetch_add(1, Relaxed);
        let dag_stages = dag.stages();
        let stages = dag_stages
            .into_iter()
            .enumerate()
            .map(|(stage_id, dag_stage)| {
                let tasks = dag_stage
                    .into_iter()
                    .enumerate()
                    .map(|(task_id, task)| {
                        Task {
                            id: task_id as _,
                            stage_id: stage_id as _,
                            dag_thread: task,
                            data_partition: 0, // unassigned
                        }
                    })
                    .collect_vec();
                Stage {
                    id: stage_id as _,
                    tasks,
                }
            })
            .collect_vec();
        let job = Job {
            id: job_id,
            stages,
            dag,
        };
        self.jobs.insert(job_id, Arc::new(job));
        return job_id;
    }
}
