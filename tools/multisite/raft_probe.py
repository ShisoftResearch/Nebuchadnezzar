"""Deterministic schedules. Assertions express the required fixed behavior."""
import json
import time


def state(gate, member):
    directory = gate.root / f'control-{member}'
    output = directory / 'raft-state.out'
    output.unlink(missing_ok=True)
    (directory / 'raft-state').touch()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if output.exists() and not (directory / 'raft-state').exists():
            return json.loads(output.read_text())
        time.sleep(.05)
    raise TimeoutError(f'local state unavailable at {member}')


def record(gate, label, members):
    result = {str(i): state(gate, i) for i in members}
    (gate.root / f'raft-{label}.json').write_text(json.dumps(result, indent=2))
    return result


def brief(states):
    """Compact failure text: ids are shortened, logs are left out."""
    out = {}
    for m, s in states.items():
        roster = (s.get('roster') or {}).get('members', [])
        out[m] = dict(term=s['term'], leader='self' if s['is_leader'] else s['leader_id'] % 10000,
                      commit=s['commit'], voters=sorted(v['id'] % 10000 for v in s['voters']),
                      roster=sorted((x['id'] % 10000, x.get('online')) for x in roster))
    return out


def run(gate):
    probe = gate.args.probe
    control = gate.args.kind == 'graceful'
    members = list(range(gate.args.members))
    before = record(gate, 'before', members)
    expected = {s['id'] for s in before.values()}
    assert all({m['id'] for m in s['roster']['members']} == expected for s in before.values()), brief(before)
    if probe == 'raft-seed':
        # The seed restart is the control for the non-seed restart.
        victim = 0 if control else 1
        gate.servers[victim].stop(abrupt=True)
        gate.start(victim)
        after = record(gate, 'after', members)
        assert all({m['id'] for m in s['voters']} == expected for s in after.values()), brief(after)
    elif probe == 'raft-heartbeat':
        gate.servers[0].stop(abrupt=True)
        survivors = members[1:]
        deadline = time.monotonic() + 60
        leader = None
        while time.monotonic() < deadline:
            states = record(gate, 'election', survivors)
            leaders = [int(i) for i, s in states.items() if s['is_leader'] and s['leader_id'] == s['id']]
            if len(leaders) == 1:
                leader = leaders[0]
                break
            time.sleep(.1)
        assert leader is not None, brief(states)
        if control:
            # Only the destination changes. Use the same unmodified ping RPC.
            for i in survivors:
                (gate.root / f'control-{i}' / 'direct-ping').write_text(gate.addresses[leader])
        time.sleep(20)  # Existing 10 s timeout + 3 checks + margin.
        after = record(gate, 'after', survivors)
        live = expected - {before['0']['id']}
        roster = after[str(leader)]['roster']['members']
        online = {m['id'] for m in roster if m['online']}
        assert live <= online, ('live members marked offline', sorted(i % 10000 for i in live), sorted(i % 10000 for i in online), brief(after))
    elif probe in ('raft-roster', 'raft-rejoin'):
        # Control: no member restarts. Every restart loses the roster of the restarted member.
        if not control:
            gate.servers[1].stop(abrupt=False)
            gate.start(1)
        after = record(gate, 'after-member', members)
        if probe == 'raft-rejoin' and not control:
            gate.servers[0].stop(abrupt=False)
            gate.start(0)
            after = record(gate, 'after-seed', members)
        assert all({m['id'] for m in s['roster']['members']} == expected for s in after.values()), brief(after)
    else:
        raise AssertionError(probe)
    (gate.root / 'PASS').write_text(probe + ' passed\n')
