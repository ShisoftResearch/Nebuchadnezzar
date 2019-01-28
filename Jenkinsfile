pipeline {
  agent any
  stages {
    stage('Rust') {
      steps {
        sh '''source $HOME/.cargo/env
rustup default nightly
rustup update
cargo clean
cargo update'''
      }
    }
    stage('Cargo build') {
      steps {
        sh 'cargo build'
      }
    }
    stage('Build check') {
      parallel {
        stage('Build check') {
          steps {
            sh '''RUST_LOG=neb=warn
export RUST_BACKTRACE=full

cargo test -- --test-threads=1'''
          }
        }
        stage('KV Parallel Stability') {
          steps {
            sh '''export RUST_LOG=neb=warn, 
export RUST_BACKTRACE=full
export NEB_KV_SMOKE_TEST_ITEMS=5000000

cargo test --color=always --package neb --test tests server::smoke_test_parallel -- --exact'''
          }
        }
        stage('KV Stability') {
          steps {
            sh '''export RUST_LOG=neb=warn, 
export RUST_BACKTRACE=full
export NEB_KV_SMOKE_TEST_ITEMS=1500000

cargo test --color=always --package neb --test tests server::smoke_test -- --exact'''
          }
        }
        stage('Bench') {
          steps {
            sh 'cargo bench'
          }
        }
        stage('B-Plus Tree Stability') {
          steps {
            sh '''export RUST_LOG=neb::ram::cleaner,neb::index::btree::test
export RUST_BACKTRACE=full
export BTREE_TEST_ITEMS=100000
export NEB_CLEANER_SLEEP_INTERVAL_MS=1000

cargo test --package neb --lib index::btree::test::crd -- --exact
cargo test --package neb --lib index::btree::test::alternative_insertion_pattern -- --exact'''
          }
        }
        stage('B-Plus Tree Parallel stability') {
          steps {
            sh '''export RUST_LOG=neb::ram::cleaner,neb::index::btree::test
export RUST_BACKTRACE=full
export BTREE_TEST_ITEMS=100000
export NEB_CLEANER_SLEEP_INTERVAL_MS=1000

cargo test --package neb --lib index::btree::test::parallel -- --exact'''
          }
        }
      }
    }
  }
}