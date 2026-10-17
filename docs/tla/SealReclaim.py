#!/usr/bin/env python3
"""Scenarios of SealReclaim.tla: one cfg each; runs TLC and checks each result."""
import os, shutil, subprocess, sys, tempfile
from pathlib import Path
# Usage: SealReclaim.py [--out DIR]
# Writes one cfg per scenario and the TLC logs to DIR (default: a fresh
# directory under $TMPDIR). Exit status 1 if any result differs.
spec = Path(__file__).resolve().parent / 'SealReclaim.tla'
args = sys.argv[1:]
here = Path(args[1]).resolve() if args[:1] == ['--out'] else Path(tempfile.mkdtemp(prefix='sealreclaim-'))
here.mkdir(parents=True, exist_ok=True)
if spec.parent != here:
    shutil.copy2(spec, here / spec.name)
TLC = os.environ.get('TLC', 'tlc')
# name: (expected, Gate, Wait, Bounded, Dead)
S = {
    'synced_wait': ('pass', 'synced', True, True, True),
    'synced_live': ('pass', 'synced', True, True, False),
    'd20': ('pass', 'd20', False, True, True),
    'ctl_no_gate': ('NoLostWrite', 'none', False, True, True),
    'ctl_no_gate_wait': ('NoLostWrite', 'none', True, True, True),
    'ctl_unbounded_wait': ('SealEnds', 'synced', True, False, True),
}
failed = 0
for name, (expect, gate, wait, bounded, dead) in S.items():
    cfg = here / f'{name}.cfg'
    b = lambda v: 'TRUE' if v else 'FALSE'
    cfg.write_text(f'''SPECIFICATION Spec
CONSTANTS
    Gate = "{gate}"
    Wait = {b(wait)}
    Bounded = {b(bounded)}
    Waits = 2
    Dead = {b(dead)}
INVARIANTS TypeOK NoLostWrite Exclusive ArchiveAfterSync
PROPERTIES SealEnds
''')
    out = subprocess.run([TLC, '-workers', '1', '-deadlock', '-config', cfg.name, spec.name],
                         cwd=here, capture_output=True, text=True).stdout
    (here / f'{name}.log').write_text(out)
    if 'Model checking completed. No error has been found.' in out:
        got = 'pass'
    elif 'Invariant NoLostWrite is violated' in out:
        got = 'NoLostWrite'
    elif 'Temporal properties were violated' in out:
        got = 'SealEnds'
    else:
        got = 'other'
    ok = got == expect
    failed += not ok
    print(f'{name}: expected {expect}, got {got}{"" if ok else "  <-- MISMATCH"}')
sys.exit(1 if failed else 0)
