use std::{
    collections::BTreeMap,
    fs,
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchMetrics {
    latency_ns: Vec<u64>,
    pub attempts: u64,
    pub committed: u64,
    pub not_realizable: u64,
    pub logical_retries: u64,
    pub unexpected: Vec<String>,
}

impl BatchMetrics {
    pub fn one_success(latency: Duration) -> Self {
        let mut metrics = Self::default();
        metrics.record_success(latency, 1, 0);
        metrics
    }

    pub fn record_success(&mut self, latency: Duration, attempts: u64, logical_retries: u64) {
        let latency_ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.latency_ns.push(latency_ns);
        self.attempts += attempts;
        self.committed += 1;
        self.logical_retries += logical_retries;
    }

    pub fn record_retryable(&mut self) {
        self.not_realizable += 1;
    }

    pub fn record_unexpected(&mut self, unexpected: impl Into<String>) {
        self.unexpected.push(unexpected.into());
    }

    pub fn merge(&mut self, other: Self) {
        self.latency_ns.extend(other.latency_ns);
        self.attempts += other.attempts;
        self.committed += other.committed;
        self.not_realizable += other.not_realizable;
        self.logical_retries += other.logical_retries;
        self.unexpected.extend(other.unexpected);
    }

    pub fn summary(&self, elapsed: Duration) -> ScenarioSummary {
        let mut latency_ns = self.latency_ns.clone();
        latency_ns.sort_unstable();

        ScenarioSummary {
            committed: self.committed,
            attempts: self.attempts,
            not_realizable: self.not_realizable,
            logical_retries: self.logical_retries,
            commits_per_second: commits_per_second(self.committed, elapsed),
            latency_p50_ns: nearest_rank_percentile(&latency_ns, 50),
            latency_p95_ns: nearest_rank_percentile(&latency_ns, 95),
            latency_p99_ns: nearest_rank_percentile(&latency_ns, 99),
            unexpected: self.unexpected.clone(),
            invariants_passed: self.unexpected.is_empty(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScenarioSummary {
    pub committed: u64,
    pub attempts: u64,
    pub not_realizable: u64,
    pub logical_retries: u64,
    pub commits_per_second: f64,
    pub latency_p50_ns: u64,
    pub latency_p95_ns: u64,
    pub latency_p99_ns: u64,
    pub unexpected: Vec<String>,
    pub invariants_passed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunReport {
    pub label: String,
    pub revision: String,
    pub scenarios: BTreeMap<String, ScenarioSummary>,
}

impl RunReport {
    pub fn new(label: impl Into<String>, revision: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            revision: revision.into(),
            scenarios: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, scenario_name: impl Into<String>, summary: ScenarioSummary) {
        self.scenarios.insert(scenario_name.into(), summary);
    }

    pub fn write_json(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = sibling_tmp_path(path)?;
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        fs::write(&tmp_path, bytes)?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

fn commits_per_second(committed: u64, elapsed: Duration) -> f64 {
    let elapsed_seconds = elapsed.as_secs_f64();
    if elapsed_seconds == 0.0 {
        0.0
    } else {
        committed as f64 / elapsed_seconds
    }
}

fn nearest_rank_percentile(sorted_latency_ns: &[u64], percentile: usize) -> u64 {
    if sorted_latency_ns.is_empty() {
        return 0;
    }

    let rank = (percentile * sorted_latency_ns.len() + 99) / 100;
    let index = rank.saturating_sub(1).min(sorted_latency_ns.len() - 1);
    sorted_latency_ns[index]
}

fn sibling_tmp_path(path: &Path) -> io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "report path must include a file name",
        )
    })?;

    Ok(path.with_file_name(format!("{}.tmp", file_name.to_string_lossy())))
}
