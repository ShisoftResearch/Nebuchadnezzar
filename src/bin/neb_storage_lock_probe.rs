use neb::server::{NebServer, ServerOptions};
use std::any::Any;
use std::env;
use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

struct ProbeArgs {
    server_addr: String,
    group_name: String,
    database_name: String,
    backup_storage: String,
    wal_storage: String,
    undo_log_storage: String,
    raft_storage: String,
    hold_secs: u64,
}

impl ProbeArgs {
    fn parse() -> Result<Self, String> {
        let mut args = env::args().skip(1);
        let server_addr = args.next().ok_or_else(|| "missing server_addr".to_string())?;
        let group_name = args.next().ok_or_else(|| "missing group_name".to_string())?;
        let database_name = args
            .next()
            .ok_or_else(|| "missing database_name".to_string())?;
        let backup_storage = args
            .next()
            .ok_or_else(|| "missing backup_storage".to_string())?;
        let wal_storage = args.next().ok_or_else(|| "missing wal_storage".to_string())?;
        let undo_log_storage = args
            .next()
            .ok_or_else(|| "missing undo_log_storage".to_string())?;
        let raft_storage = args.next().ok_or_else(|| "missing raft_storage".to_string())?;
        let hold_secs = args
            .next()
            .ok_or_else(|| "missing hold_secs".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("invalid hold_secs: {e}"))?;

        Ok(Self {
            server_addr,
            group_name,
            database_name,
            backup_storage,
            wal_storage,
            undo_log_storage,
            raft_storage,
            hold_secs,
        })
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn main() -> ExitCode {
    let args = match ProbeArgs::parse() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("ERROR: {error}");
            return ExitCode::from(64);
        }
    };

    let result = std::panic::catch_unwind(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        runtime.block_on(async_main(args))
    });

    match result {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(error)) => {
            eprintln!("ERROR: {error}");
            ExitCode::from(1)
        }
        Err(payload) => {
            eprintln!("PANIC: {}", panic_message(payload));
            ExitCode::from(2)
        }
    }
}

async fn async_main(args: ProbeArgs) -> Result<(), String> {
    let server = NebServer::new_from_opts_in_database(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(args.backup_storage.clone()),
            wal_storage: Some(args.wal_storage.clone()),
            undo_log_storage: Some(args.undo_log_storage.clone()),
            raft_storage: Some(args.raft_storage.clone()),
            index_enabled: false,
            services: vec![],
            enable_recovery: false,
        },
        &args.server_addr,
        &args.group_name,
        &args.database_name,
        async |_| {},
    )
    .await
    .map_err(|e| e.to_string())?;

    println!("READY pid={}", std::process::id());
    std::io::stdout()
        .flush()
        .map_err(|e| format!("failed to flush READY line: {e}"))?;

    tokio::time::sleep(Duration::from_secs(args.hold_secs)).await;
    server.shutdown().await;
    Ok(())
}