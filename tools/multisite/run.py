#!/usr/bin/env python3
"""Real-process gate. Run this controller inside the required systemd scope."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import selectors
import signal
import shutil
import socket
import struct
import subprocess
import threading
import time
from check import check_transaction, check_batch, check_protocol_events, is_transport_error


def append(path, value):
    with path.open('a') as stream:
        stream.write(json.dumps(value, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


class Proxy:
    """Forward complete length-delimited frames. Never corrupt the TCP stream."""
    def __init__(self, port, destination, name, ledger):
        self.destination, self.name, self.ledger = destination, name, ledger
        self.delay = 0
        self.drop = 0
        self.cut = False
        self.closed = False
        self.frames = 0
        self.connections = set()
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', port))
        self.listener.listen()
        self.listener.settimeout(.2)
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while not self.closed:
            try:
                source, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.lock:
                overloaded = len(self.connections) >= 256
            if overloaded:
                append(self.ledger, {'event':'proxy_capacity_failure', 'link':self.name})
                source.close()
                continue
            try:
                target = socket.create_connection(self.destination, timeout=2)
                source.settimeout(None)
                target.settimeout(None)
            except OSError:
                source.close()
                continue
            with self.lock:
                self.connections.update((source, target))
            threading.Thread(target=self.pump, args=(source, target), daemon=True).start()
            threading.Thread(target=self.pump, args=(target, source), daemon=True).start()

    @staticmethod
    def exact(sock, length):
        data = bytearray()
        while len(data) < length:
            part = sock.recv(length-len(data))
            if not part:
                raise EOFError()
            data.extend(part)
        return bytes(data)

    def pump(self, source, target):
        try:
            while not self.closed:
                header = self.exact(source, 4)
                length, = struct.unpack('>I', header)
                if length > 64*1024*1024:
                    raise ValueError('oversized frame')
                frame = self.exact(source, length)
                with self.lock:
                    self.frames += 1
                    drop = self.cut or self.drop > 0
                    if self.drop:
                        self.drop -= 1
                    delay = self.delay
                if drop:
                    append(self.ledger, {'event':'drop', 'link':self.name, 'bytes':length})
                    continue
                if delay:
                    time.sleep(delay)
                target.sendall(header + frame)
        except (EOFError, OSError, ValueError):
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
                with self.lock:
                    self.connections.discard(sock)

    def close(self):
        self.closed = True
        self.listener.close()
        with self.lock:
            for sock in list(self.connections):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


class Process:
    def __init__(self, gate, role, index, args):
        self.gate, self.role, self.index = gate, role, index
        generation = gate.generations.get((role,index), 0)
        gate.generations[role,index] = generation+1
        self.log = gate.root / f'{role}-{index}-{generation}.jsonl'
        self.stderr = (gate.root / f'{role}-{index}-{generation}.stderr').open('w')
        env = os.environ.copy()
        env.update(MULTISITE_ROUTES=gate.routes(index), NEB_DISABLE_JOIN_FILL='1',
                   RUST_BACKTRACE='1', RUST_LOG=os.environ.get('RUST_LOG','warn'), MULTISITE_EVENTS=str(gate.root / f'protocol-{index}.jsonl'),
                   MULTISITE_CONTROL=str(gate.root / f'control-{index}'))
        self.proc = subprocess.Popen([str(gate.binary)] + args, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.stderr, env=env, bufsize=0)
        gate.children.append(self)
        append(gate.root/'pids.jsonl', {'event':'start','pid':self.proc.pid,'role':role,'index':index,'args':args})
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.proc.stdout, selectors.EVENT_READ)
        self.buffer = b''
        self.wait_event('ready', 90)

    def event(self, timeout):
        deadline = time.monotonic()+timeout
        while b'\n' not in self.buffer:
            if not self.selector.select(max(0, deadline-time.monotonic())):
                raise TimeoutError(f'{self.role} {self.index} stdout timeout: {self.log}')
            data = os.read(self.proc.stdout.fileno(), 65536)
            if not data:
                raise RuntimeError(f'{self.role} {self.index} exited: {self.proc.poll()}; {self.log}')
            self.buffer += data
        line, self.buffer = self.buffer.split(b'\n', 1)
        try:
            event = json.loads(line)
        except ValueError:
            event = {'event':'output','line':line.decode(errors='replace')}
        append(self.log, event)
        if event.get('event') in ('begin','prepared'):
            append(self.gate.root/'ledger.jsonl',event)
            if event['event'] == 'begin':
                self.gate.tids[str(event['id'])] = event['tid']
        return event

    def wait_event(self, kind, timeout=90):
        deadline = time.monotonic()+timeout
        while True:
            result = self.event(max(0, deadline-time.monotonic()))
            if result.get('event') == kind:
                return result

    def send(self, request):
        append(self.gate.root/'ledger.jsonl', {'event':'intent', 'request':request})
        self.proc.stdin.write((json.dumps(request)+'\n').encode())
        self.proc.stdin.flush()

    def result(self, timeout=90):
        result = self.wait_event('result', timeout)
        append(self.gate.root/'ledger.jsonl', result)
        return result['result']

    def command(self, request, timeout=90):
        self.send(request)
        return self.result(timeout)

    def stop(self, abrupt=False):
        if self.proc.poll() is None:
            sig = signal.SIGKILL if abrupt else signal.SIGTERM
            append(self.gate.root/'pids.jsonl', {'event':'signal','pid':self.proc.pid,'signal':int(sig)})
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                append(self.gate.root/'pids.jsonl', {'event':'shutdown_timeout','pid':self.proc.pid})
                self.proc.kill()
                self.proc.wait()
        append(self.gate.root/'pids.jsonl', {'event':'exit','pid':self.proc.pid,'code':self.proc.returncode})
        self.stderr.close()


class Gate:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.output).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.binary = Path(args.binary).resolve()
        assert self.binary.is_file() and os.access(self.binary, os.X_OK), self.binary
        self.children, self.proxies, self.generations, self.servers = [], {}, {}, {}
        self.pause_offsets = {}
        self.tids = {}
        self.port_lock = open(f'/mnt/ext/morpheus-fixes/multisite/ports-{args.port}.lock', 'w')
        fcntl.flock(self.port_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        count = args.members
        # Reserve and check the whole range before any process starts.
        reservations = []
        try:
            for port in range(args.port, args.port+100):
                sock = socket.socket()
                sock.bind(('127.0.0.1',port))
                reservations.append(sock)
        finally:
            for sock in reservations:
                sock.close()
        self.addresses = [f'127.0.0.1:{args.port+i}' for i in range(count)]
        self.group = 'multisite'
        receipt = {'binary':str(self.binary),'sha256':hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                   'head':args.head,'seed':args.seed,'members':count,'port_range':[args.port,args.port+99],
                   'controller_pid':os.getpid()}
        (self.root/'receipt.json').write_text(json.dumps(receipt, indent=2))
        append(self.root/'pids.jsonl', {'event':'controller', 'pid':os.getpid()})
        for source in range(count+1):
            for target in range(count):
                port = args.port+10+source*count+target
                self.proxies[source,target] = Proxy(port, ('127.0.0.1', args.port+target),
                    f'{source}->{target}', self.root/'network.jsonl')

    def routes(self, source):
        return ';'.join(f'{address}=127.0.0.1:{self.args.port+10+source*self.args.members+target}'
                        for target,address in enumerate(self.addresses))

    def start(self, index):
        store = self.root/f'store-{index}'
        store.mkdir(exist_ok=True)
        (self.root/f'control-{index}').mkdir(exist_ok=True)
        # Initial nodes join the already bootstrapped members. Restarts retain all identities.
        peers = self.addresses[:index+1] if not (store/'raft').exists() else self.addresses
        process = Process(self, 'server', index, ['server', self.addresses[index], ','.join(peers),self.group,str(store)])
        self.servers[index] = process

    def client(self):
        (self.root/f'control-{self.args.members}').mkdir(exist_ok=True)
        return Process(self, 'client', self.args.members,
            ['client',f'127.0.0.1:{self.args.port+8}',','.join(self.addresses),self.group])

    def close(self):
        for process in reversed(self.children):
            process.stop()
        for proxy in self.proxies.values():
            proxy.close()
        (self.root/'proxy-counts.json').write_text(json.dumps({p.name:p.frames for p in self.proxies.values()}, indent=2))

    def arm(self, index, point):
        directory = self.root/f'control-{index}'
        directory.mkdir(exist_ok=True)
        for name in ('resume','claimed'):
            (directory/name).unlink(missing_ok=True)
        path = self.root/f'protocol-{index}.jsonl'
        self.pause_offsets[index] = path.stat().st_size if path.exists() else 0
        (directory/'arm').write_text(point)

    def wait_pause(self, index, point):
        path = self.root/f'protocol-{index}.jsonl'
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            if path.exists():
                with path.open() as source:
                    source.seek(self.pause_offsets[index])
                    lines = source.read().splitlines()
                for line in reversed(lines):
                    event = json.loads(line)
                    if event['point'] == point and event['paused']:
                        append(self.root/'events.jsonl', {'event':'pause_observed', 'protocol':event})
                        return event
            if self.servers[index].proc.poll() is not None:
                raise RuntimeError(f'member {index} stopped before {point}')
            time.sleep(.01)
        raise TimeoutError(f'point {point} was not reached on {index}')

    # ---- mixed schedule ----
    WORKLOADS = ['s1','s2','s3','abort','conflict1','conflict8','conflict64']
    FAULTS = ['point-kill','point-graceful','time-kill','time-graceful','cut','delay','drop']
    SINGLE_POINTS = ['before_p0','after_p0','during_end','coordinator_before_end']
    MULTI_POINTS = ['before_p0','after_p0','during_end','before_status_record','after_status_record',
                    'before_status_release','coordinator_before_end','coordinator_after_end']

    def build_request(self, rng, event, workload):
        members = self.args.members
        durable = self.args.durable
        if workload.startswith('conflict'):
            count = int(workload[len('conflict'):])
            sites = rng.sample(range(members), 2)
            keys = [[91+m, 100000+event] for m in sites]
            requests = [{'op':'txn','id':f'{event}.{i}','keys':keys,'value':event*1000+i+1,'durable':durable,
                         'coordinator':rng.randrange(members)} for i in range(count)]
            return {'op':'batch','requests':requests,'id':str(event)}, sites
        count = min({'s1':1,'s2':2,'s3':3,'abort':2}[workload], members)
        sites = rng.sample(range(members), count)
        keys = [[91+m, 100000+event] for m in sites]
        request = {'op':'txn','id':f'{event}.0','keys':keys,'value':event*1000+1,'durable':durable,
                   'coordinator':rng.randrange(members)}
        if workload == 'abort':
            request['abort'] = True
        return request, sites

    def set_links(self, victim, **fault):
        for (source, target), proxy in self.proxies.items():
            if victim in (source, target) and source != target:
                for name, value in fault.items():
                    setattr(proxy, name, value)

    def run_event(self, event, rng, client, counters):
        workload = self.WORKLOADS[event % len(self.WORKLOADS)]
        if workload == 's3' and self.args.members < 3:
            workload = 's2'
        fault = self.FAULTS[(event + event//len(self.WORKLOADS)) % len(self.FAULTS)]
        request, sites = self.build_request(rng, event, workload)
        singles = [request] if request['op'] == 'txn' else request['requests']
        victim = rng.choice(sites)
        point = None
        if fault.startswith('point'):
            if workload == 'abort':
                fault = 'time-' + fault.split('-')[1]
            else:
                point = rng.choice(self.SINGLE_POINTS if workload == 's1' else self.MULTI_POINTS)
                if point.startswith('coordinator_'):
                    victim = rng.randrange(self.args.members)
                    for single in singles:
                        single['coordinator'] = victim
        record = {'event':event,'workload':workload,'fault':fault,'victim':victim,'point':point,'sites':sites}
        if point:
            self.arm(victim, point)
            client.send(request)
            try:
                self.wait_pause(victim, point)
            except TimeoutError:
                (self.root/f'control-{victim}'/'arm').unlink(missing_ok=True)
                record['skipped'] = 'point not reached'
                counters['skipped'] += 1
                append(self.root/'events.jsonl', record)
                return request, self.collect(client, request, record)
            kind = fault.split('-')[1]
            append(self.root/'events.jsonl', record | {'kind':kind})
            if kind == 'graceful':
                process = self.servers[victim]
                append(self.root/'pids.jsonl', {'event':'signal','pid':process.proc.pid,'signal':15,'point':point})
                process.proc.terminate()
                (self.root/f'control-{victim}'/'resume').touch()
                process.stop()
            else:
                self.servers[victim].stop(abrupt=True)
            (self.root/f'control-{victim}'/'arm').unlink(missing_ok=True)
            self.start(victim)
        elif fault.startswith('time'):
            kind = fault.split('-')[1]
            client.send(request)
            time.sleep(rng.uniform(0, .3))
            append(self.root/'events.jsonl', record | {'kind':kind})
            self.servers[victim].stop(abrupt=kind == 'kill')
            self.start(victim)
        else:
            client.send(request)
            time.sleep(rng.uniform(0, .1))
            append(self.root/'events.jsonl', record | {'kind':fault})
            if fault == 'cut':
                self.set_links(victim, cut=True)
                time.sleep(rng.uniform(2, 6))
            elif fault == 'delay':
                self.set_links(victim, delay=.1)
                time.sleep(rng.uniform(2, 6))
            else:
                self.set_links(victim, drop=3)
                time.sleep(1)
            self.set_links(victim, cut=False, delay=0, drop=0)
        counters['events'] += 1
        counters[fault] += 1
        return request, self.collect(client, request, record)

    def collect(self, client, request, record):
        try:
            return client.result(240)
        except (TimeoutError, RuntimeError) as error:
            # The client call did not return. The outcome is unknown (D33).
            append(self.root/'events.jsonl', record | {'client_lost':repr(error)})
            lost = {'outcome':'unknown','client_lost':True}
            if request['op'] == 'txn':
                return lost
            return {'results':[lost for _ in request['requests']]}

    def read_all(self, client, keysets, deadline=150):
        """Read each key set. Retry locked or unreachable reads: an in-doubt cell may resolve."""
        end = time.monotonic()+deadline
        while True:
            observed = [client.command({'op':'read','keys':keys}, 120)['values'] for keys in keysets]
            if not any('error' in item for values in observed for item in values) or time.monotonic() > end:
                return observed
            time.sleep(5)

    def verify(self, client, operations):
        observed = self.read_all(client, [op['keys'] for op in operations])
        for op, values in zip(operations, observed):
            if op['kind'] == 'batch':
                check_batch(op['requests'], op['results'], values)
            else:
                check_transaction(op['requests'][0], op['results'][0], values)

    def audit(self, client, tids, label):
        """Stuck locks, brackets and in-doubt transactions after a heal. Waits one lease period first."""
        time.sleep(35)
        report = {'label':label,'transactions':len(tids),'problems':[]}
        for index in range(self.args.members):
            control = self.root/f'control-{index}'
            for name in ('stats','stats.tmp'):
                (control/name).unlink(missing_ok=True)
            (control/'inspect.tmp').write_text(json.dumps(tids))
            (control/'inspect.tmp').rename(control/'inspect')
            deadline = time.monotonic()+60
            while not (control/'stats').exists():
                assert time.monotonic() < deadline, f'member {index} did not answer inspect'
                time.sleep(.1)
            stats = json.loads((control/'stats').read_text())
            if stats['unresolved'] or stats['pinned_segments']:
                report['problems'].append({'member':index,'unresolved':stats['unresolved'],'pinned':stats['pinned_segments']})
            for item in stats['transactions']:
                if item['leases'] or item['bracket_status'] == 'Prepared':
                    report['problems'].append({'member':index,'tid':item['tid'],'leases':item['leases'],'bracket':item['bracket_status']})
        for tid in tids:
            statuses = {index: client.command({'op':'status','site':index,'tid':tid}, 60)['status']
                        for index in range(self.args.members)}
            if any(v in ('Committing','InDoubt') for v in statuses.values()):
                report['problems'].append({'tid':tid,'stuck_status':statuses})
            if 'Committed' in statuses.values() and 'Aborted' in statuses.values():
                report['problems'].append({'tid':tid,'split_decision':statuses})
        append(self.root/'audits.jsonl', report)
        assert not report['problems'], ('stuck state after heal', report['problems'][:5])

    def run_mixed(self, client):
        rng = random.Random(self.args.seed)
        operations, audited = [], 0
        counters = {'events':0,'skipped':0, **{name:0 for name in self.FAULTS}}
        event = 0
        while counters['events'] < self.args.events and event < self.args.events*3:
            request, result = self.run_event(event, rng, client, counters)
            requests = request['requests'] if request['op'] == 'batch' else [request]
            results = result['results'] if request['op'] == 'batch' else [result]
            operations.append({'kind':request['op'],'requests':requests,'results':results,'keys':requests[0]['keys']})
            append(self.root/'ledger.jsonl', {'event':'outcome','id':event,'outcomes':[r['outcome'] for r in results]})
            client.stop(abrupt=True)
            client = self.client()
            self.verify(client, operations if (event+1) % 50 == 0 else operations[-25:])
            append(self.root/'checks.jsonl', {'event':event,'counted':counters['events'],'result':'pass'})
            event += 1
            if counters['events'] and counters['events'] % 50 == 0 and counters['events'] != audited:
                audited = counters['events']
                self.audit(client, list(self.tids.values())[-200:], f'after {audited} events')
        self.audit(client, list(self.tids.values())[-400:], 'final')
        self.verify(client, operations)
        events = [json.loads(line) for index in range(self.args.members)
                  if (self.root/f'protocol-{index}.jsonl').exists()
                  for line in (self.root/f'protocol-{index}.jsonl').read_text().splitlines()]
        counters['protocol'] = check_protocol_events(events)
        outcomes = {}
        for op in operations:
            for r in op['results']:
                name = 'commit_error_transport' if is_transport_error(r) else r['outcome']
                outcomes[name] = outcomes.get(name,0)+1
        counters['outcomes'] = outcomes
        (self.root/'summary.json').write_text(json.dumps(counters, indent=2))
        assert counters['events'] >= self.args.events, ('too few events', counters)
        (self.root/'PASS').write_text('mixed campaign passed\n')

    def run(self):
        for index in range(self.args.members):
            self.start(index)
        client = self.client()
        assert client.command({'op':'setup'})['ok']
        if self.args.probe == 'readonly-e2e':
            initial = client.command({'op':'txn','id':'initial','keys':[[92,900]],'value':1,'durable':True})
            assert initial['outcome'] == 'ack', initial
            first = client.command({'op':'txn','id':'reader-writer','keys':[[91,901]],'reads':[[92,900]],
                                    'value':2,'durable':self.args.durable})
            time.sleep(35)
            second = client.command({'op':'txn','id':'conflicting-write','keys':[[92,900]],'value':3,'durable':True})
            report = {'first':first,'second':second}
            (self.root/'probe-e2e.json').write_text(json.dumps(report,indent=2))
            assert first['outcome'] == 'ack', report
            assert second['outcome'] == 'ack', ('read-only participant left the cell locked', report)
            (self.root/'PASS').write_text('read-only e2e probe passed\n')
            return
        if self.args.probe in ('readonly','readonly-control'):
            initial = client.command({'op':'txn','id':'initial','keys':[[92,900]],'value':1,'durable':True})
            assert initial['outcome'] == 'ack', initial
            keys = [[91,901]] if self.args.probe == 'readonly' else [[91,901],[92,901]]
            result = client.command({'op':'txn','id':'readonly-probe','keys':keys,'reads':[[92,900]],
                                     'value':2,'end_site':1})
            (self.root/'probe-result.json').write_text(json.dumps(result,indent=2))
            time.sleep(35)
            status = client.command({'op':'status','site':1,'tid':result['tid']})
            (self.root/'probe-status.json').write_text(json.dumps(status,indent=2))
            assert result['end'] == 'Success', ('read-only end must finish', result, status)
            assert status['status'] != 'Committing', ('end remains stuck',status)
            (self.root/'PASS').write_text('read-only end probe passed\n')
            return
        if self.args.probe == 'restart':
            request = {'op':'txn','id':'restart-probe','keys':[[91+i,1] for i in range(self.args.members)],
                       'value':77,'durable':True}
            result = client.command(request)
            assert result['outcome'] == 'ack', result
            report = {'before':client.command({'op':'owners'}),'victim':self.args.victim,'kind':self.args.kind}
            self.servers[self.args.victim].stop(abrupt=self.args.kind == 'kill')
            self.start(self.args.victim)
            time.sleep(self.args.settle)
            client.stop(abrupt=True)
            client = self.client()
            report['after'] = client.command({'op':'owners'})
            report['values'] = client.command({'op':'read','keys':request['keys']})['values']
            (self.root/'probe-restart.json').write_text(json.dumps(report,indent=2))
            check_transaction(request, result, report['values'])
            (self.root/'PASS').write_text('restart probe passed\n')
            return
        if self.args.schedule == 'mixed':
            return self.run_mixed(client)
        rng = random.Random(self.args.seed)
        operations = []
        points = ['after_p0','during_end','before_status_record','after_status_record',
                  'before_status_release','coordinator_before_end','coordinator_after_end']
        for event in range(self.args.events):
            sites = 1+event%3 if self.args.schedule == 'baseline' else 2+event%2
            keys = [[91+i,event+1] for i in range(sites)]
            request = {'op':'txn','id':event,'keys':keys,'value':event+1000,'durable':self.args.durable}
            victim = rng.randrange(sites)
            kind = 'none' if self.args.no_faults else ['kill','graceful','cut','delay','drop'][event%5]
            if self.args.schedule == 'protocol':
                point = points[event%len(points)]
                if point.startswith('coordinator_'):
                    victim = 0
                self.arm(victim, point)
                client.send(request)
                pause = self.wait_pause(victim, point)
                kind = 'kill' if event%2 == 0 else 'graceful'
                append(self.root/'events.jsonl', {'event':event,'kind':kind,'victim':victim,'point':point})
                if kind == 'graceful':
                    process = self.servers[victim]
                    append(self.root/'pids.jsonl', {'event':'signal','pid':process.proc.pid,'signal':15,'point':point})
                    process.proc.terminate()
                    (self.root/f'control-{victim}'/'resume').touch()
                    process.stop()
                else:
                    self.servers[victim].stop(abrupt=True)
                (self.root/f'control-{victim}'/'arm').unlink(missing_ok=True)
                self.start(victim)
                result = client.result()
                # The protocol lease timeout is 30 s. This is a correctness bound, not timing data.
                time.sleep(35)
            else:
                result = client.command(request)
                assert result['outcome'] == 'ack', result
                append(self.root/'events.jsonl', {'event':event,'kind':kind,'victim':victim})
                if kind in ('kill','graceful'):
                    self.servers[victim].stop(abrupt=kind=='kill')
                    self.start(victim)
                elif kind != 'none':
                    source = (victim+1)%self.args.members
                    proxy = self.proxies[source,victim]
                    if kind == 'cut':
                        proxy.cut = True
                    elif kind == 'delay':
                        proxy.delay = .1
                    else:
                        proxy.drop = 3
                    time.sleep(3)
                    proxy.cut, proxy.delay, proxy.drop = False, 0, 0
            operations.append((request,result))
            client.stop(abrupt=True)
            client = self.client()
            for old_request, old_result in operations:
                values = client.command({'op':'read','keys':old_request['keys']})['values']
                check_transaction(old_request,old_result,values)
            append(self.root/'checks.jsonl', {'event':event,'operations':len(operations),'result':'pass'})
        (self.root/'PASS').write_text('baseline restart checks passed\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--head', required=True, help='Immutable source receipt, not an automatic checkout')
    parser.add_argument('--output', required=True)
    parser.add_argument('--members', type=int, choices=[2,3,5], default=3)
    parser.add_argument('--port', type=int, default=46100)
    parser.add_argument('--seed', type=int, default=6601)
    parser.add_argument('--events', type=int, default=200)
    parser.add_argument('--durable', action='store_true')
    parser.add_argument('--no-faults', action='store_true')
    parser.add_argument('--probe', choices=['none','readonly','readonly-control','readonly-e2e','restart'], default='none')
    parser.add_argument('--victim', type=int, default=1)
    parser.add_argument('--kind', choices=['kill','graceful'], default='graceful')
    parser.add_argument('--settle', type=float, default=5)
    parser.add_argument('--schedule', choices=['baseline','protocol','mixed'], default='baseline')
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    gate = Gate(args)
    try:
        gate.run()
    except BaseException as error:
        (gate.root/'FAIL').write_text(repr(error)+'\n')
        raise
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        gate.close()
        if (gate.root/'FAIL').exists():
            image = gate.root/'failure-image'
            image.mkdir()
            for store in gate.root.glob('store-*'):
                shutil.copytree(store, image/store.name)


if __name__ == '__main__':
    main()
