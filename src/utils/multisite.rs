//! Test-only process events and one-shot pause points.
//! The controller must remove `resume` before it arms a point.
use std::{
    io::Write,
    path::PathBuf,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
static EVENTS: Mutex<()> = Mutex::new(());

pub fn point(name: &str, detail: impl std::fmt::Debug) {
    let Ok(path) = std::env::var("MULTISITE_EVENTS") else {
        return;
    };
    let control = std::env::var("MULTISITE_CONTROL").ok().map(PathBuf::from);
    let armed = control.as_ref().is_some_and(|dir| {
        let arm = dir.join("arm");
        std::fs::read_to_string(&arm).is_ok_and(|point| point.trim() == name)
            && std::fs::rename(arm, dir.join("claimed")).is_ok()
    });
    {
        let _guard = EVENTS.lock().unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(
            file,
            "{}",
            serde_json::json!({
                "pid": std::process::id(), "point": name, "detail": format!("{detail:?}"),
                "ns": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string(),
                "paused": armed,
            })
        )
        .unwrap();
        file.sync_data().unwrap();
    }
    if armed {
        let control = control.unwrap();
        while !control.join("resume").exists() {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
