import socket
import struct
import tempfile
import threading
import time
import unittest
from pathlib import Path
from check import check_transaction
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
