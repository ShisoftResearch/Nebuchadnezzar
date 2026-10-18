import socket
import struct
import tempfile
import threading
import time
import unittest
from pathlib import Path
from check import check_transaction, check_batch, check_protocol_events
from run import Proxy


class LedgerTests(unittest.TestCase):
    def test_ack_and_unknown_controls(self):
        request = {'keys':[[91,1],[92,1]],'value':7}
        all_present = [{'value':7}]*2
        all_absent = [{'absent':True}]*2
        check_transaction(request, {'outcome':'ack'}, all_present)
        check_transaction(request, {'outcome':'unknown'}, all_present)
        check_transaction(request, {'outcome':'unknown'}, all_absent)
        for outcome, values in [('ack',all_absent),('unknown',[all_present[0],all_absent[1]]),
                                ('abort',all_present),('unknown',[{'error':'Timeout'}]*2),
                                ('commit_error',all_absent)]:
            with self.assertRaises(AssertionError):
                check_transaction(request, {'outcome':outcome}, values)


class BatchAndProtocolTests(unittest.TestCase):
    def test_per_key_values(self):
        request = {'keys':[[91,1],[92,1]],'values':[5,6],'value':0}
        check_transaction(request, {'outcome':'ack'}, [{'value':5},{'value':6}])
        for observed in ([{'value':5},{'absent':True}], [{'value':6},{'value':5}]):
            with self.assertRaises(AssertionError):
                check_transaction(request, {'outcome':'ack'}, observed)

    def test_batch_controls(self):
        requests = [{'keys':[[91,1],[92,1]],'value':v} for v in (1,2,3)]
        results = [{'outcome':'ack'},{'outcome':'prepare_error'},{'outcome':'unknown'}]
        check_batch(requests, results, [{'value':1}]*2)
        check_batch(requests, results, [{'value':3}]*2)
        for observed in ([{'value':2}]*2, [{'value':1},{'value':3}], [{'absent':True}]*2, [{'value':1},{'absent':True}]):
            with self.assertRaises(AssertionError):
                check_batch(requests, results, observed)
        check_batch(requests, [{'outcome':'unknown'}]*3, [{'absent':True}]*2)
        with self.assertRaises(AssertionError):
            check_batch(requests, [{'outcome':'commit_error','error':'StoreFull'}]*3, [{'absent':True}]*2)

    def test_protocol_controls(self):
        good = [{'point':'before_status_release','detail':{'participants':[1,2],'synced_peers':[2],'self_synced':True}},
                {'point':'status_answer','detail':{'self_synced':True}}]
        self.assertEqual(check_protocol_events(good)['status_release'], 1)
        for bad in ({'point':'before_status_release','detail':{'participants':[1,2],'synced_peers':[],'self_synced':True}},
                    {'point':'before_status_release','detail':{'participants':[1,2],'synced_peers':[2],'self_synced':False}},
                    {'point':'status_answer','detail':{'self_synced':False}}):
            with self.assertRaises(AssertionError):
                check_protocol_events([bad])


class ProxyTests(unittest.TestCase):
    def test_complete_frames_drop_cut_restore_delay(self):
        with tempfile.TemporaryDirectory() as directory:
            listener = socket.socket()
            listener.bind(('127.0.0.1',0))
            listener.listen()
            def echo():
                connection, _ = listener.accept()
                try:
                    while True:
                        data = connection.recv(65536)
                        if not data:
                            break
                        connection.sendall(data)
                except OSError:
                    pass
                finally:
                    connection.close()
            thread = threading.Thread(target=echo, daemon=True)
            thread.start()
            proxy = Proxy(0, listener.getsockname(), 'control', Path(directory)/'network.jsonl')
            client = socket.create_connection(proxy.listener.getsockname())
            client.settimeout(.2)
            frame = struct.pack('>I',8)+b'12345678'
            try:
                client.sendall(frame)
                self.assertEqual(Proxy.exact(client,12),frame)
                for fault in ('drop','cut'):
                    setattr(proxy,fault,1)
                    client.sendall(frame)
                    with self.assertRaises(socket.timeout):
                        client.recv(1)
                    setattr(proxy,fault,0)
                    client.sendall(frame)
                    self.assertEqual(Proxy.exact(client,12),frame)
                proxy.delay = .03
                start = time.monotonic()
                client.sendall(frame)
                self.assertEqual(Proxy.exact(client,12),frame)
                self.assertGreaterEqual(time.monotonic()-start,.05)
            finally:
                client.close()
                proxy.close()
                listener.close()
                thread.join(1)


if __name__ == '__main__':
    unittest.main()
