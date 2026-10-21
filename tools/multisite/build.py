#!/usr/bin/env python3
"""Build an immutable gate binary from a selected sibling workspace."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--workspace', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--release', action='store_true')
args = parser.parse_args()
workspace = Path(args.workspace).resolve()
output = Path(args.output).resolve()
output.mkdir(parents=True, exist_ok=False)
env = os.environ.copy()
env.update(CARGO_TARGET_DIR='/mnt/optane/cargo-target/multisite',
           RUSTFLAGS='--cfg disable_shortcut --cfg multisite_test', TMPDIR='/mnt/ext/morpheus-fixes/multisite/tmp')
receipt = {'workspace':str(workspace),'environment':{k:env[k] for k in ('CARGO_TARGET_DIR','RUSTFLAGS')}}
for repo in ('Nebuchadnezzar','Morpheus','bifrost','Dovahkiin'):
    directory = workspace/repo
    head = subprocess.check_output(['git','rev-parse','HEAD'],cwd=directory,text=True).strip()
    status = subprocess.check_output(['git','status','--porcelain'],cwd=directory,text=True)
    if status:
        raise RuntimeError(f'Commit the source before an immutable build: {repo}\n{status}')
    receipt[repo] = head
(output/'source.json').write_text(json.dumps(receipt,indent=2))
with (output/'cargo.jsonl').open('w') as stdout, (output/'cargo.stderr').open('w') as stderr:
    result = subprocess.run(['flock','/mnt/ext/morpheus-fixes/cargo.lock','cargo','build',
            '--example','multisite_endpoint','--message-format=json','-j','10'] + (['--release'] if args.release else []),
            cwd=workspace/'Nebuchadnezzar',env=env,stdout=stdout,stderr=stderr)
(output/'exit').write_text(str(result.returncode)+'\n')
if result.returncode:
    raise SystemExit(result.returncode)
artifacts = []
for line in (output/'cargo.jsonl').read_text().splitlines():
    try:
        item = json.loads(line)
    except ValueError:
        continue
    if item.get('reason') == 'compiler-artifact' and item.get('executable') and item['target']['name'] == 'multisite_endpoint':
        artifacts.append(item['executable'])
if len(artifacts) != 1 or not os.access(artifacts[0],os.X_OK):
    (output/'parser-failure.json').write_text(json.dumps(artifacts))
    raise RuntimeError('Missing or ambiguous build artifact')
shutil.copy2(artifacts[0], output/'endpoint')
receipt['sha256'] = hashlib.sha256((output/'endpoint').read_bytes()).hexdigest()
(output/'receipt.json').write_text(json.dumps(receipt,indent=2))
(output/'DONE').touch()
