#!/usr/bin/env python3
"""Adapter for the repository's isolated runner; never compiles."""
import os
from pathlib import Path
import sys

names = ['multisite_readonly', 'multisite_readonly_control', 'multisite_controller', 'multisite_smoke_3', 'multisite_smoke_5', 'multisite_seed_restart_2', 'multisite_restart_probe', 'multisite_readonly_e2e', 'multisite_mixed_3', 'multisite_mixed_5', 'multisite_cut_prepare', 'multisite_abandon', 'multisite_partial_abort']
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
    if name == 'multisite_restart_probe':
        members = int(os.environ.get('MULTISITE_MEMBERS', '2'))
        args[args.index('--members')+1] = str(members)
        args += ['--probe','restart','--victim',os.environ.get('MULTISITE_VICTIM','1'),
                 '--kind',os.environ.get('MULTISITE_KIND','graceful'),'--steps',os.environ.get('MULTISITE_STEPS','')]
    if name == 'multisite_cut_prepare':
        args += ['--probe','cut-prepare','--cut-after',os.environ.get('MULTISITE_CUT_AFTER','0.05'),
                 '--cut-point',os.environ.get('MULTISITE_CUT_POINT',''),'--cut-hold',os.environ.get('MULTISITE_CUT_HOLD','5'),'--cut-member',os.environ.get('MULTISITE_CUT_MEMBER','-1'),'--wait',os.environ.get('MULTISITE_WAIT','0')]
    if name == 'multisite_partial_abort':
        args += ['--probe','partial-abort','--cut-member',os.environ.get('MULTISITE_CUT_MEMBER','-1')]
    if name == 'multisite_abandon':
        args += ['--probe','abandon','--cut-member',os.environ.get('MULTISITE_CUT_MEMBER','-1')]
    if name == 'multisite_readonly_e2e':
        args += ['--probe','readonly-e2e']
    elif name.startswith('multisite_readonly'):
        args += ['--probe','readonly-control' if name.endswith('_control') else 'readonly']
    if os.environ.get('MULTISITE_NO_FAULTS') == '1':
        args.append('--no-faults')
    if os.environ.get('MULTISITE_DURABLE') == '1':
        args.append('--durable')
    if name.startswith('multisite_mixed'):
        args += ['--schedule','mixed']
    if os.environ.get('MULTISITE_FAULTS'):
        args += ['--faults',os.environ['MULTISITE_FAULTS']]
    if os.environ.get('MULTISITE_LEASE_SECS'):
        args += ['--lease-secs',os.environ['MULTISITE_LEASE_SECS']]
    if os.environ.get('MULTISITE_WORKLOADS'):
        args += ['--workloads',os.environ['MULTISITE_WORKLOADS']]
    if os.environ.get('MULTISITE_AUDIT_EVERY'):
        args += ['--audit-every',os.environ['MULTISITE_AUDIT_EVERY']]
    if os.environ.get('MULTISITE_SAFE_STOPS') == '1':
        args.append('--safe-stops')
    if os.environ.get('MULTISITE_NO_ORPHAN_ADOPTION') == '1':
        args.append('--no-orphan-adoption')
    if os.environ.get('MULTISITE_NO_REFRESH') == '1':
        args.append('--no-refresh')
    if os.environ.get('MULTISITE_NO_SEED_STOP') == '1':
        args.append('--no-seed-stop')
    if 'MULTISITE_SEED' in os.environ:
        args += ['--seed',os.environ['MULTISITE_SEED']]
    if 'MULTISITE_SCHEDULE' in os.environ:
        args += ['--schedule',os.environ['MULTISITE_SCHEDULE']]
    os.execv(sys.executable,args)
