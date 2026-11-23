"""Проверки TCP через независимые socket-клиенты, без сторонних библиотек."""
import argparse
from pathlib import Path
import signal
import select
import socket
import subprocess
import time
import unittest

parser = argparse.ArgumentParser()
parser.add_argument('--build-dir', type=Path, required=True)
args = parser.parse_args()
build = args.build_dir.resolve()
root = Path(__file__).resolve().parents[1]


class Protocol(unittest.TestCase):
    def setUp(self):
        self.children, self.sockets = [], []
        self.addCleanup(self.cleanup)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.port = probe.getsockname()[1]
        self.server = self.spawn('server', '127.0.0.1', str(self.port), str(root/'data/minimal.conf'))
        self.connect()

    def spawn(self, role, *argv):
        p = subprocess.Popen([str(build/('crossroad_'+role)), *argv],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.children.append(p)
        return p

    def cleanup(self):
        for s in self.sockets:
            s.close()
        for p in self.children:
            if p.poll() is None:
                p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                self.fail('process did not exit on SIGTERM')
            finally:
                p.stdout.close()

    def connect(self):
        end = time.monotonic() + 3
        while True:
            try:
                s = socket.create_connection(('127.0.0.1', self.port), timeout=.5)
                s.settimeout(2)
                self.sockets.append(s)
                return s
            except ConnectionRefusedError:
                if time.monotonic() >= end:
                    raise
                time.sleep(.01)  # Только ожидание доступности сервера, не порядок событий.

    def line(self, s):
        data = b''
        while not data.endswith(b'\n'):
            part = s.recv(1)
            if not part:
                break
            data += part
        return data

    def test_fragmented_and_coalesced(self):
        s = self.connect()
        for b in b'HELLO 1 FLOW A\n':
            s.sendall(bytes([b]))
        self.assertEqual(self.line(s), b'WELCOME 0 39 1000 0 20\n')
        s.sendall(b'PING\nPING\n')
        self.assertEqual(self.line(s), b'PONG\n')
        self.assertEqual(self.line(s), b'PONG\n')
        other = self.connect()
        other.sendall(b'HELLO 1 FLOW A\n')
        self.assertEqual(self.line(other), b'ERROR occupied\n')

    def test_bad_frames_are_isolated(self):
        for frame in [b'HELLO 2 WATCH\n', b'HELLO 1 OTHER\n', b'HELLO 1 FLOW X\n',
                      b'HELLO 1 WATCH extra\n', b'\0\n', b'x'*1024+b'\n']:
            s = self.connect()
            s.sendall(frame)
            try:
                response = self.line(s)
            except ConnectionResetError:
                response = b''
            self.assertTrue(not response or response.startswith(b'ERROR'), response)
        good = self.connect()
        good.sendall(b'HELLO 1 WATCH\nPING\n')
        self.assertEqual(self.line(good), b'WELCOME WATCH\n')
        self.assertEqual(self.line(good), b'PONG\n')

    def test_incomplete_peer_and_timeout(self):
        slow = self.connect()
        slow.sendall(b'HELLO 1 FLO')
        good = self.connect()
        good.sendall(b'HELLO 1 FLOW P\n')
        self.assertTrue(self.line(good).startswith(b'WELCOME 1 '))
        slow.shutdown(socket.SHUT_WR)
        self.assertEqual(slow.recv(1), b'')
        silent = self.connect()
        silent.settimeout(7)
        self.assertEqual(silent.recv(1), b'')

    def test_connection_refused(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
            # Bind без listen держит порт занятым, connect получает отказ.
            client = self.spawn('client', '127.0.0.1', str(port), 'A')
            self.assertEqual(client.wait(timeout=12), 1)

    def test_cli_and_port_conflict(self):
        for role, argv in [('server', ['127.0.0.1', str(self.port), str(root/'data/minimal.conf')]),
                           ('client', ['not-an-ip', '3939', 'A']),
                           ('server', ['127.0.0.1', '65536', 'x'])]:
            p = self.spawn(role, *argv)
            self.assertIn(p.wait(timeout=2), (1, 2))
        client = self.spawn('client', '127.0.0.1', str(self.port), 'A')
        self.assertTrue(select.select([client.stdout], [], [], 2)[0])
        self.assertTrue(client.stdout.readline().startswith(b'WELCOME 0 '))
        self.assertIsNone(client.poll())
        client.send_signal(signal.SIGTERM)
        self.assertEqual(client.wait(timeout=2), 128 + signal.SIGTERM)


if __name__ == '__main__':
    unittest.main(argv=['protocol'])
