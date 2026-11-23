"""Проверки TCP через независимые socket-клиенты, без сторонних библиотек."""
import argparse
from pathlib import Path
import signal
import select
import socket
import subprocess
import time
import unittest
import tempfile
import re
from check_log import verify

parser = argparse.ArgumentParser()
parser.add_argument('--build-dir', type=Path, required=True)
parser.add_argument('--suite', choices=['protocol', 'scenarios', 'observers'], default='protocol')
args = parser.parse_args()
build = args.build_dir.resolve()
root = Path(__file__).resolve().parents[1]


class Harness(unittest.TestCase):
    def setUp(self):
        self.children, self.sockets = [], []
        self.addCleanup(self.cleanup)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.port = probe.getsockname()[1]
        self.server = self.spawn('server', '127.0.0.1', str(self.port), str(root/'data/minimal.conf'))
        self.connect()

    def spawn(self, role, *argv, output=subprocess.PIPE):
        p = subprocess.Popen([str(build/('crossroad_'+role)), *argv],
                             stdout=output, stderr=subprocess.DEVNULL)
        self.children.append(p)
        return p

    def cleanup(self):
        stuck = []
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
                stuck.append(p.pid)
            finally:
                if p.stdout:
                    p.stdout.close()
        self.assertFalse(stuck, f'processes did not exit on SIGTERM: {stuck}')

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

class Protocol(Harness):
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
        while True:
            row = self.line(good)
            self.assertTrue(row)
            if row.startswith(b'SNAP_END '):
                break
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

    def test_arrival_validation(self):
        a, pedestrian = self.connect(), self.connect()
        a.sendall(b'HELLO 1 FLOW A\n')
        pedestrian.sendall(b'HELLO 1 FLOW P\n')
        self.assertTrue(self.line(a).startswith(b'WELCOME '))
        self.assertTrue(self.line(pedestrian).startswith(b'WELCOME '))
        a.sendall(b'ARRIVE 18446744073709551615 0\n')
        pedestrian.sendall(b'ARRIVE 0 1\n')
        for peer, error in [(a, b'ERROR tick\n'), (pedestrian, b'ERROR command\n')]:
            while True:
                line = self.line(peer)
                if not line or line.startswith(b'ERROR'):
                    self.assertEqual(line, error)
                    break

    def test_half_close_preserves_reply(self):
        peer = self.connect()
        peer.sendall(b'HELLO 1 FLOW A\n')
        peer.shutdown(socket.SHUT_WR)
        self.assertEqual(self.line(peer), b'WELCOME 0 39 1000 0 20\n')
        self.assertEqual(peer.recv(1), b'')

    def test_blocked_client_stdout(self):
        with socket.socket() as fake:
            fake.bind(('127.0.0.1', 0))
            fake.listen(1)
            fake.settimeout(3)
            client = self.spawn('client', '127.0.0.1', str(fake.getsockname()[1]), 'A')
            connection, _ = fake.accept()
            with connection:
                connection.settimeout(2)
                self.assertEqual(self.line(connection), b'HELLO 1 FLOW A\n')
                try:
                    connection.sendall(b'WELCOME 0 39 1000 0 20\n' + b'EVENT 1 0 0 test\n' * 20000)
                except (ConnectionResetError, BrokenPipeError, socket.timeout):
                    pass
                self.assertEqual(client.wait(timeout=2), 1)

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


class Scenarios(Harness):
    def run_model(self, strategy='adaptive', target=6, extra='', rates=(1000, 1000, 1000), expected=0):
        self.server.terminate()
        self.server.wait(timeout=2)
        with tempfile.TemporaryDirectory(dir=build) as tmp:
            directory = Path(tmp)
            config = (f'ticks 0\ntarget {target}\nstrategy {strategy}\ntick_ms 20\n'
                      'min_green 2\nmax_green 4\nclearance 1\nemergency_rate 500\n'
                      f'stream A car north straight 2 2 {rates[0]}\n'
                      f'stream B car south straight 2 2 {rates[1]}\n'
                      f'stream P pedestrian east walk 2 1 {rates[2]}\n'
                      'conflict A B 0\nphase cars A B\nphase pedestrians P\n' + extra)
            path = directory/'input.conf'
            path.write_text(config)
            files = [open(directory/(name+'.log'), 'w+b') for name in ['server','A','B','P']]
            try:
                server = self.spawn('server', '127.0.0.1', str(self.port), str(path), output=files[0])
                self.connect()
                clients = [self.spawn('client', '127.0.0.1', str(self.port), name, output=files[i+1])
                           for i, name in enumerate(['A','B','P'])]
                self.assertEqual(server.wait(timeout=8), expected)
                for client in clients:
                    self.assertIn(client.wait(timeout=3), (0, 1))
                text = ''
                for f in files:
                    f.seek(0)
                    text += f.read().decode()
                summary = verify(text, config)
                if summary['passed']:
                    with self.assertRaises(AssertionError):
                        verify(re.sub(r'срок=\d+', 'срок=0', text, count=1), config)
                    with self.assertRaises(AssertionError):
                        verify(re.sub(r'ИТОГО: прибыло=\d+', 'ИТОГО: прибыло=0', text), config)
                if summary['simultaneous']:
                    with self.assertRaises(AssertionError):
                        verify(text, config.replace('conflict A B 0', 'conflict A B 1'))
                return summary
            finally:
                for f in files:
                    f.close()

    def test_exact_target(self):
        self.assertEqual(self.run_model()['passed'], 6)

    def test_fast_ticks(self):
        self.assertEqual(self.run_model(extra='tick_ms 1\n')['passed'], 6)

    def test_fixed_and_compatible(self):
        summary = self.run_model(strategy='fixed', target=20)
        self.assertEqual(summary['passed'], 20)
        self.assertTrue(summary['simultaneous'])
        self.assertGreater(summary['refused'], 0)

    def test_period_and_zero_flow(self):
        summary = self.run_model(target=0, extra='ticks 30\n', rates=(1000, 1000, 0))
        self.assertGreater(summary['passed'], 0)

    def test_busy_peer_does_not_block_period(self):
        self.server.terminate()
        self.server.wait(timeout=2)
        with tempfile.TemporaryDirectory(dir=build) as tmp:
            path = Path(tmp)/'flood.conf'
            path.write_text('ticks 100\ntick_ms 2\ndelay_ms 1000\n'
                            'stream A car north straight 1 1 0\nphase a A\n')
            server = self.spawn('server', '127.0.0.1', str(self.port), str(path))
            s = self.connect()
            s.sendall(b'HELLO 1 FLOW A\n')
            self.assertTrue(self.line(s).startswith(b'WELCOME '))
            # Избыточный трафик не должен вытеснить серверный период.
            try:
                s.sendall(b'PING\n' * 40000)
            except (ConnectionResetError, BrokenPipeError):
                pass
            self.assertEqual(server.wait(timeout=2), 0)
            self.assertIn('Завершение: причина=1', server.stdout.read().decode())

    def test_unreachable(self):
        self.assertEqual(self.run_model(rates=(0, 0, 0), expected=3)['arrived'], 0)


class Observers(Harness):
    def snapshot(self):
        peer = self.connect()
        peer.sendall(b'HELLO 1 WATCH\n')
        self.assertEqual(self.line(peer), b'WELCOME WATCH\n')
        rows = []
        while True:
            row = self.line(peer).decode().strip()
            self.assertTrue(row)
            rows.append(row)
            if row.startswith('SNAP_END '):
                break
        begin = next(r for r in rows if r.startswith('SNAP_BEGIN ')).split()
        self.assertEqual(rows[-1], 'SNAP_END ' + begin[1])
        self.assertEqual(len([r for r in rows if r.startswith('FLOW ')]), 2)
        return peer, rows, int(begin[1])

    def test_initial_snapshot(self):
        _, rows, seq = self.snapshot()
        self.assertEqual(seq, 0)
        self.assertIn('STATE 0 0 0 0 0 0', rows)

    def events(self, peer):
        rows = []
        while True:
            line = self.line(peer).decode().strip()
            if not line:
                return rows
            if line.startswith('EVENT '):
                rows.append(line)

    def test_observers_reconnect_and_log(self):
        first, _, _ = self.snapshot()
        second, _, _ = self.snapshot()
        third, _, _ = self.snapshot()
        slow, _, _ = self.snapshot()  # Дальнейшие события намеренно не читаются.
        with tempfile.TemporaryDirectory(dir=build) as tmp:
            path = Path(tmp)/'watch.log'
            observer = self.spawn('observer', '127.0.0.1', str(self.port), str(path),
                                  output=subprocess.DEVNULL)
            deadline = time.monotonic() + 2
            while not path.exists() or 'SNAP_END ' not in path.read_text():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.01)
            for name in ['A', 'P']:
                self.spawn('client', '127.0.0.1', str(self.port), name, output=subprocess.DEVNULL)
            prefix = []
            while not any('Разрешение:' in r for r in prefix):
                prefix.append(self.line(first).decode().strip())
            second.close()
            late, snapshot, boundary = self.snapshot()
            self.assertGreater(boundary, 0)
            total = 0
            for row in snapshot:
                if row.startswith('FLOW '):
                    f = list(map(int, row.split()[1:]))
                    self.assertEqual(f[5], f[6] + f[7] + f[1] + int(f[3] != 0))
                    total += f[5]
            self.assertGreater(total, 0)
            all_events = prefix + self.events(first)
            self.assertEqual(all_events, self.events(third))
            later = self.events(late)
            self.assertEqual(later, [r for r in all_events if int(r.split()[1]) > boundary])
            self.assertEqual(self.server.wait(timeout=2), 0)
            self.assertEqual(observer.wait(timeout=2), 1)  # STOP появится в этапе 6.
            full = path.read_text()
            self.assertGreater(verify(full, (root/'data/minimal.conf').read_text())['passed'], 0)
            before = full
            duplicate = self.spawn('observer', '127.0.0.1', str(self.port), str(path))
            self.assertEqual(duplicate.wait(timeout=2), 1)
            self.assertEqual(path.read_text(), before)

    def test_connection_limit(self):
        for _ in range(63):
            self.connect()
        excess = self.connect()
        self.assertEqual(excess.recv(1), b'')
        healthy = self.sockets[0]
        healthy.sendall(b'HELLO 1 WATCH\n')
        self.assertEqual(self.line(healthy), b'WELCOME WATCH\n')



if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        {'protocol': Protocol, 'scenarios': Scenarios, 'observers': Observers}[args.suite])
    raise SystemExit(not unittest.TextTestRunner().run(suite).wasSuccessful())
