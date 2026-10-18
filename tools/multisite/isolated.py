#!/usr/bin/env python3
"""Adapter for the repository's isolated runner; never compiles."""
import os
from pathlib import Path
import sys

names = ['multisite_readonly', 'multisite_readonly_control', 'multisite_controller', 'multisite_smoke_3', 'multisite_smoke_5', 'multisite_seed_restart_2']
if '--list' in sys.argv:
    for name in names:
        print(name + ': test')
else:
    name = sys.argv[1]
    assert name in names
    if name == 'multisite_controller':
        os.execv(sys.executable, [sys.executable, str(Path(os.environ['MULTISITE_TOOLS'])/'test_controller.py')])
    binary = Path(os.environ['MULTISITE_BINARY'])
    assert binary.is_file() and os.access(binary, os.X_OK), binary
    members = 5 if name.endswith('_5') else 2 if name.endswith('_2') else 3
    args = [sys.executable, str(Path(os.environ['MULTISITE_TOOLS'])/'run.py'), '--binary',str(binary),
            '--head',os.environ['MULTISITE_HEAD'],'--output',str(Path(os.environ['MULTISITE_OUTPUT'])/name),
            '--members',str(members),'--events',os.environ.get('MULTISITE_EVENTS_COUNT','2'),
            '--port',os.environ.get('MULTISITE_PORT','46100')]
    if name.startswith('multisite_readonly'):
        args += ['--probe','readonly-control' if name.endswith('_control') else 'readonly']
    if os.environ.get('MULTISITE_NO_FAULTS') == '1':
        args.append('--no-faults')
    if os.environ.get('MULTISITE_DURABLE') == '1':
        args.append('--durable')
    if 'MULTISITE_SCHEDULE' in os.environ:
        args += ['--schedule',os.environ['MULTISITE_SCHEDULE']]
    os.execv(sys.executable,args)
