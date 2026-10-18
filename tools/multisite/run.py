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
from check import check_transaction


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

    def run(self):
        for index in range(self.args.members):
            self.start(index)
        client = self.client()
        assert client.command({'op':'setup'})['ok']
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
    parser.add_argument('--probe', choices=['none','readonly','readonly-control','restart'], default='none')
    parser.add_argument('--victim', type=int, default=1)
    parser.add_argument('--kind', choices=['kill','graceful'], default='graceful')
    parser.add_argument('--settle', type=float, default=5)
    parser.add_argument('--schedule', choices=['baseline','protocol'], default='baseline')
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
