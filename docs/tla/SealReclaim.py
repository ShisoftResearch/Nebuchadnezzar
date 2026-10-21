#!/usr/bin/env python3
"""Scenarios of SealReclaim.tla: one cfg each; runs TLC and checks each result."""
import os, re, shutil, subprocess, sys, tempfile
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
ALL = 'TypeOK NoLostWrite NoLostLateWrite Exclusive ArchiveAfterSync'
# name: (expected, Gate, Wait, Bounded, Dead, CloseGate, OtherChecks, invariants)
# A violation scenario checks one invariant, so that TLC names the same one each time.
S = {
    'synced_wait': ('pass', 'synced', True, True, True, True, True, ALL),
    'synced_live': ('pass', 'synced', True, True, False, True, True, ALL),
    'synced_rule_only': ('pass', 'synced', True, True, True, True, False, ALL),
    'd20': ('pass', 'd20', False, True, True, False, True, ALL),
    'ctl_synced_no_close': ('NoLostLateWrite', 'synced', True, True, True, False, True, 'NoLostLateWrite'),
    'ctl_none': ('NoLostLateWrite', 'none', False, True, True, False, True, 'NoLostLateWrite'),
    'ctl_no_gate': ('NoLostWrite', 'none', False, True, True, False, False, 'NoLostWrite'),
    'ctl_no_gate_wait': ('NoLostWrite', 'none', True, True, True, False, False, 'NoLostWrite'),
    'ctl_unbounded_wait': ('SealEnds', 'synced', True, False, True, True, True, ALL),
}
failed = 0
for name, (expect, gate, wait, bounded, dead, close, other, invariants) in S.items():
    cfg = here / f'{name}.cfg'
    b = lambda v: 'TRUE' if v else 'FALSE'
    cfg.write_text(f'''SPECIFICATION Spec
CONSTANTS
    Gate = "{gate}"
    Wait = {b(wait)}
    Bounded = {b(bounded)}
    Waits = 2
    Dead = {b(dead)}
    CloseGate = {b(close)}
    OtherChecks = {b(other)}
INVARIANTS {invariants}
PROPERTIES SealEnds
''')
    out = subprocess.run([TLC, '-workers', '1', '-deadlock', '-config', cfg.name, spec.name],
                         cwd=here, capture_output=True, text=True).stdout
    (here / f'{name}.log').write_text(out)
    found = re.findall(r'(\d+) states generated, (\d+) distinct states found', out)
    states = found[-1][1] if found else '?'
    if 'Model checking completed. No error has been found.' in out:
        got = 'pass'
    elif 'Invariant NoLostLateWrite is violated' in out:
        got = 'NoLostLateWrite'
    elif 'Invariant NoLostWrite is violated' in out:
        got = 'NoLostWrite'
    elif 'Temporal properties were violated' in out:
        got = 'SealEnds'
    else:
        got = 'other'
    ok = got == expect
    failed += not ok
    print(f'{name}: expected {expect}, got {got}, {states} distinct states{"" if ok else "  <-- MISMATCH"}')
sys.exit(1 if failed else 0)
