#!/usr/bin/env python3
"""Scenarios of HeadWordLock.tla: one MC module and cfg each; runs TLC."""
import json, os, shutil, subprocess, sys, re, time
from pathlib import Path
# Usage: HeadWordLock.py [--out DIR] [scenario ...]
# Writes MC_<scenario>.tla/.cfg and the TLC logs to DIR (default: a fresh
# directory under $TMPDIR), runs TLC, and compares each result with the
# expected one. Exit status 1 if any result differs.
spec = Path(__file__).resolve().parent / 'HeadWordLock.tla'
args = sys.argv[1:]
if args[:1] == ['--out']:
    here = Path(args[1]).resolve(); args = args[2:]
else:
    import tempfile
    here = Path(tempfile.mkdtemp(prefix='headwordlock-'))
here.mkdir(parents=True, exist_ok=True)
if spec.parent != here:
    shutil.copy2(spec, here / spec.name)
TLC = os.environ.get('TLC', 'tlc')
# proc: (kind, cell, pred, walgo)
S = {}
def sc(name, expect, procs, exists, k, algo, ubounded=False, full=False,
       fast_bounded=True, recheck=True, liveness=True, note=''):
    S[name] = dict(expect=expect, procs=procs, exists=exists, k=k, algo=algo,
                   ubounded=ubounded, full=full, fast_bounded=fast_bounded,
                   recheck=recheck, liveness=liveness, note=note)
D = lambda c='x', pred=True: ('D', c, pred, 'headfirst')
W = lambda c='x', alg='headfirst': ('W', c, True, alg)
U = lambda c='x': ('U', c, True, 'headfirst')
C = lambda c='x': ('C', c, True, 'headfirst')
T = lambda c='x': ('T', c, True, 'headfirst')
X = {'x': True}
XY = {'x': True, 'y': True}
# 1. e6a64539 deadlock: delete holds the word and waits for the head.
sc('old_k1', 'deadlock', {'d1': D(), 'w1': W()}, X, 1, 'old', note='e6a64539 cycle, 1 head')
sc('old_k2', 'deadlock', {'d1': D(), 'w1': W(), 'w2': W()}, X, 2, 'old', note='e6a64539 cycle, 2 heads')
# 2. Current order (9020529d).
sc('cur_k1', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'c1': C()}, X, 1, 'cur')
sc('cur_k2', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'w2': W(), 'c1': C(), 't1': T()}, X, 2, 'cur')
sc('cur_k2_two_cells', 'pass', {'d1': D('x'), 'd2': D('y'), 'w1': W('y'), 'w2': W('x'), 'c1': C('y'), 't1': T('x')}, XY, 2, 'cur')
# 2b. Pre-existing cycle C1: word-first updater, no deadline away from the wall.
sc('cur_u_unbounded', 'deadlock', {'u1': U(), 'w1': W(), 'w2': W()}, X, 2, 'cur', ubounded=False, note='control: C1 without the deadline')
sc('cur_u_bounded', 'pass', {'u1': U(), 'd1': D(), 'w1': W(), 'w2': W()}, X, 2, 'cur', ubounded=True)
# 3. D35 candidate: one word acquisition on the fast path.
sc('new_k1', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'c1': C()}, X, 1, 'new')
sc('new_k2', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'w2': W(), 'c1': C(), 't1': T()}, X, 2, 'new')
sc('new_k2_u', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'u1': U(), 'c1': C(), 't1': T()}, X, 2, 'new', ubounded=True)
sc('new_k2_two_cells', 'pass', {'d1': D('x'), 'd2': D('y'), 'w1': W('y'), 'w2': W('x'), 'c1': C('y'), 't1': T('x')}, XY, 2, 'new')
sc('new_k3', 'pass', {'d1': D(), 'd2': D(), 'w1': W(), 'w2': W(), 'w3': W(), 't1': T()}, X, 3, 'new')
sc('new_u_bounded_c1', 'pass', {'u1': U(), 'w1': W(), 'w2': W()}, X, 2, 'new', ubounded=True,
   note='C1 shape with the head-queue deadline for best-effort callers')
sc('new_u_bounded_c1_k3', 'pass', {'u1': U(), 'w1': W(), 'w2': W(), 'w3': W(), 'd1': D()}, X, 3, 'new', ubounded=True)
sc('new_u_unbounded', 'deadlock', {'u1': U(), 'd1': D(), 'w1': W(), 'w2': W()}, X, 2, 'new', ubounded=False,
   note='control: C1 without the deadline')
# 3b. Reverted controls of the candidate.
sc('ctl_new_unbounded_try', 'deadlock', {'d1': D(), 'w1': W()}, X, 1, 'new', fast_bounded=False,
   note='control: remove the bound of the fast try')
sc('ctl_new_no_recheck', 'invariant', {'d1': D(), 'd2': D(), 'w1': W()}, X, 1, 'new', recheck=False,
   liveness=False, note='control: remove the check under the second word')
# 4. Full store: an absent cell or a false predicate is refused as absent.
full_procs = {'d1': D('x'), 'd2': D('y', pred=False), 'd3': D('z')}
full_exists = {'x': False, 'y': True, 'z': True}
for a in ['old', 'cur', 'new']:
    sc(f'full_{a}', 'pass', full_procs, full_exists, 2, a, full=True)
# 5. CAS candidate (second target): word first with the same bounded try.
sc('cas_wordfast_k2', 'pass', {'w1': W(alg='wordfast'), 'w2': W(alg='wordfast'), 'w3': W(), 'd1': D(), 'c1': C(), 't1': T()}, X, 2, 'new')
sc('cas_wordfast_k1', 'pass', {'w1': W(alg='wordfast'), 'w2': W(), 'd1': D(), 'c1': C()}, X, 1, 'new')

def write(name, s):
    procs = s['procs']
    def fn(idx, default):
        arms = ' [] '.join(f'p = {p} -> {json.dumps(v[idx]) if isinstance(v[idx], str) else str(v[idx]).upper()}' for p, v in procs.items())
        return f'CASE {arms}'
    cells = s['exists']
    ex = ' [] '.join(f'c = "{c}" -> {str(v).upper()}' for c, v in cells.items())
    mod = f'''---- MODULE MC_{name} ----
EXTENDS HeadWordLock
CONSTANTS {', '.join(procs)}
MCProcs == {{{', '.join(procs)}}}
MCHeads == 1..{s['k']}
MCKind(p) == {fn(0, None)}
MCCell(p) == {fn(1, None)}
MCPred(p) == {fn(2, None)}
MCWAlgo(p) == {fn(3, None)}
MCExists(c) == CASE {ex} [] OTHER -> FALSE
====
'''
    cfg = f'''CONSTANTS
  {chr(10).join(f'{p} = {p}' for p in procs)}
  NoOne = NoOne
  Procs <- MCProcs
  Heads <- MCHeads
  KindOf <- MCKind
  CellOf <- MCCell
  PredOf <- MCPred
  WAlgoOf <- MCWAlgo
  Exists0 <- MCExists
  DeleteAlgo = "{s['algo']}"
  UBounded = {str(s['ubounded']).upper()}
  StoreFull = {str(s['full']).upper()}
  FastBounded = {str(s['fast_bounded']).upper()}
  SlowRecheck = {str(s['recheck']).upper()}
SPECIFICATION Spec
INVARIANTS TypeOK AppendUnderBoth ResultCodes LeaseSpare
{'PROPERTIES Termination' if s['liveness'] else ''}
CHECK_DEADLOCK TRUE
'''
    (here / f'MC_{name}.tla').write_text(mod)
    (here / f'MC_{name}.cfg').write_text(cfg)

def run(name):
    log = here / 'logs' / f'{name}.log'
    log.parent.mkdir(exist_ok=True)
    t = time.time()
    out = subprocess.run([TLC, '-workers', '4', '-cleanup',
                          '-metadir', str(here / 'states' / name), f'MC_{name}.tla'],
                         cwd=here, capture_output=True, text=True)
    log.write_text(out.stdout + out.stderr)
    txt = out.stdout
    if 'Deadlock reached' in txt: got = 'deadlock'
    elif re.search(r'Invariant \w+ is violated', txt): got = 'invariant'
    elif 'Temporal properties were violated' in txt: got = 'liveness'
    elif 'Model checking completed. No error has been found' in txt: got = 'pass'
    else: got = 'error'
    states = re.findall(r'(\d[\d,]*) distinct states found', txt)
    viol = re.findall(r'Invariant (\w+) is violated', txt)
    return dict(got=got, distinct_states=states[-1] if states else None, violated=viol,
                secs=round(time.time() - t, 1), log=str(log))

names = args or list(S)
res = {}
for n in names:
    write(n, S[n]); r = run(n); r.update(expect=S[n]['expect'], note=S[n]['note'],
                                          ok=(r['got'] == S[n]['expect']))
    res[n] = r; print(n, json.dumps(r), flush=True)
old = json.loads((here / 'results.json').read_text()) if (here / 'results.json').exists() else {}
old.update(res); (here / 'results.json').write_text(json.dumps(old, indent=2))
print('output:', here)
sys.exit(0 if all(r['ok'] for r in res.values()) else 1)
