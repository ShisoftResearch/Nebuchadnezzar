use core_affinity::CoreId;
use itertools::Itertools;
use lightning::{aarc::Arc, map::PtrHashMap};
use std::sync::atomic::Ordering::Relaxed;
use std::{
    pin::Pin,
    sync::atomic::{AtomicU64, AtomicUsize},
    thread,
};
use tokio::{sync::mpsc::*, task::LocalSet};

use super::Stage;

#[derive(Serialize, Deserialize, Debug)]
pub struct Task {
    id: u64,
    host: u64,
    stage_id: u64,
    data_partition: u64,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct StageId {
    id: u64,
    host: u64
}

type TaskMap = PtrHashMap<u64, Arc<Task>>;
type StageMap = PtrHashMap<StageId, Arc<Stage>>;

struct Executer {
    tasks: Arc<TaskMap>,
    stages: Arc<StageMap>,
    pending_tasks: UnboundedSender<u64>,
    current_task: AtomicU64,
    core_id: CoreId,
}

pub struct Worker {
    tasks: Arc<TaskMap>,
    stages: Arc<StageMap>,
    execs: Vec<Arc<Executer>>,
    worker_task_counter: AtomicU64,
}

impl Worker {
    pub fn new() -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let capacity = core_ids.len().next_power_of_two();
        let tasks = Arc::new(PtrHashMap::with_capacity(capacity));
        let stages = Arc::new(PtrHashMap::with_capacity(16));
        let execs = core_ids
            .into_iter()
            .map(|core_id| {
                let (sender, receiver) = unbounded_channel();
                let exec = Arc::new(Executer::new(sender, tasks.clone(), stages.clone(), core_id));
                let exec_cpy = exec.clone();
                thread::Builder::new()
                    .name(format!("QueryExec_{}", core_id.id))
                    .spawn(move || {
                        core_affinity::set_for_current(core_id);
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        let local = LocalSet::new();
                        local.spawn_local(Executer::run(exec_cpy, receiver));
                        rt.block_on(local);
                    })
                    .unwrap();
                exec
            })
            .collect_vec();
        Self {
            worker_task_counter: AtomicU64::new(0),
            stages,
            tasks,
            execs,
        }
    }

    fn new_task(&self, task: Task) {
        let new_id = self.worker_task_counter.fetch_add(1, Relaxed);
        let exec_id = new_id as usize % self.execs.len();
        self.tasks.insert_no_rt(new_id, Arc::new(task));
        self.execs[exec_id].push_task(new_id);
    }

    fn new_stage(&self, stage: Stage, host: u64, id: u64) {
        let stage_id = StageId::new(host, id);
        self.stages.insert_no_rt(stage_id, Arc::new(stage));
    }
    fn obsolete(&self, host: u64, id: u64) {
        let stage_id = StageId::new(host, id);
        self.stages.remove_rt_ref(&stage_id);
    }
}

impl Executer {
    fn new(
        pending: UnboundedSender<u64>,
        tasks: Arc<TaskMap>,
        stages: Arc<StageMap>,
        core_id: CoreId,
    ) -> Self {
        Self {
            current_task: AtomicU64::new(0),
            pending_tasks: pending,
            tasks,
            stages,
            core_id,
        }
    }
    async fn run(this: Arc<Self>, mut receiver: UnboundedReceiver<u64>) {
        while let Some(task_id) = receiver.recv().await {
            let task = this.tasks.get_ref(&task_id).unwrap();
            unimplemented!()
        }
    }

    fn push_task(&self, task_id: u64) {
        self.pending_tasks.send(task_id).unwrap()
    }
}

impl StageId {
    fn new(host: u64, id: u64) -> Self {
        StageId {
            host, id
        }
    }
}