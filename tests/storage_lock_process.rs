#[cfg(unix)]
mod tests {
    use std::fs;
    use std::io::{BufRead, BufReader, Read};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use tempfile::TempDir;

    const HOLD_SECS: &str = "120";

    struct ProbePaths {
        _root: TempDir,
        backup: PathBuf,
        wal: PathBuf,
        undo: PathBuf,
        raft: PathBuf,
    }

    impl ProbePaths {
        fn new() -> Self {
            let root = tempfile::TempDir::new().expect("tempdir should be created");
            Self {
                backup: root.path().join("backup"),
                wal: root.path().join("wal"),
                undo: root.path().join("undo"),
                raft: root.path().join("raft"),
                _root: root,
            }
        }

        fn lock_file(&self, group: &str, database: &str) -> PathBuf {
            let lock_dir = if group == database {
                self.raft.clone()
            } else {
                self.raft.join("databases").join(database)
            };
            lock_dir.join(".neb.lock")
        }
    }

    struct ProbeChild {
        child: Child,
    }

    impl ProbeChild {
        fn spawn(paths: &ProbePaths, server_addr: &str, group: &str, database: &str) -> Self {
            let child = Command::new(env!("CARGO_BIN_EXE_neb_storage_lock_probe"))
                .arg(server_addr)
                .arg(group)
                .arg(database)
                .arg(&paths.backup)
                .arg(&paths.wal)
                .arg(&paths.undo)
                .arg(&paths.raft)
                .arg(HOLD_SECS)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("probe process should spawn");

            Self { child }
        }

        fn wait_ready(&mut self) -> Result<u32, String> {
            let mut line = String::new();
            let bytes = {
                let stdout = self
                    .child
                    .stdout
                    .as_mut()
                    .expect("probe child stdout should be piped");
                let mut reader = BufReader::new(stdout);
                reader
                    .read_line(&mut line)
                    .map_err(|e| format!("failed reading child stdout: {e}"))?
            };

            if bytes == 0 {
                let mut stderr = String::new();
                self.child
                    .stderr
                    .as_mut()
                    .expect("probe child stderr should be piped")
                    .read_to_string(&mut stderr)
                    .expect("stderr should be readable");
                let status = self.child.wait().expect("child wait should succeed");
                return Err(format!(
                    "probe exited before READY: status={status}, stderr={stderr}"
                ));
            }

            let ready = line.trim();
            let pid = ready
                .strip_prefix("READY pid=")
                .ok_or_else(|| format!("unexpected READY line: {ready}"))?
                .parse::<u32>()
                .map_err(|e| format!("invalid READY pid: {e}"))?;
            Ok(pid)
        }

        fn kill(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn reserve_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("port bind should succeed");
        let addr = listener
            .local_addr()
            .expect("local addr should be available");
        drop(listener);
        addr.to_string()
    }

    fn read_lock_pid(lock_path: &Path) -> u32 {
        let contents = fs::read_to_string(lock_path).expect("lock file should be readable");
        contents
            .lines()
            .find_map(|line| line.strip_prefix("pid="))
            .expect("lock file should contain pid")
            .parse::<u32>()
            .expect("pid should parse")
    }

    #[test]
    fn second_process_is_rejected_while_first_holds_storage_lock() {
        let paths = ProbePaths::new();
        let group = "storage_lock_process_group";
        let database = "wikidata";

        let mut first = ProbeChild::spawn(&paths, &reserve_addr(), group, database);
        let first_pid = first.wait_ready().expect("first probe should become ready");
        assert_eq!(read_lock_pid(&paths.lock_file(group, database)), first_pid);

        let second = Command::new(env!("CARGO_BIN_EXE_neb_storage_lock_probe"))
            .arg(reserve_addr())
            .arg(group)
            .arg(database)
            .arg(&paths.backup)
            .arg(&paths.wal)
            .arg(&paths.undo)
            .arg(&paths.raft)
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("second probe should run");

        let stderr = String::from_utf8_lossy(&second.stderr);
        assert!(
            !second.status.success(),
            "second process should fail while first process holds the lock"
        );
        assert!(
            stderr.contains(&format!("already locked by pid {first_pid}")),
            "second process stderr should report the first process pid, got: {stderr}"
        );

        first.kill();
    }

    #[test]
    fn stale_lock_file_is_reclaimed_after_holder_process_exits() {
        let paths = ProbePaths::new();
        let group = "storage_lock_stale_group";
        let database = "wikidata";

        let mut first = ProbeChild::spawn(&paths, &reserve_addr(), group, database);
        let first_pid = first.wait_ready().expect("first probe should become ready");
        assert_eq!(read_lock_pid(&paths.lock_file(group, database)), first_pid);

        first.kill();

        let mut second = ProbeChild::spawn(&paths, &reserve_addr(), group, database);
        let second_pid = second.wait_ready().expect("second probe should reclaim stale lock");
        assert_ne!(first_pid, second_pid);
        assert_eq!(read_lock_pid(&paths.lock_file(group, database)), second_pid);

        second.kill();
    }
}