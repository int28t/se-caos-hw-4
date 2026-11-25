"""Независимое восстановление движения из совокупности локальных журналов."""
import re


def verify(text, config):
    records, streams, phases, conflicts = {}, {}, {}, {}
    settings = dict(min_green=4, max_green=10, clearance=2, tick_ms=1)
    for row in config.splitlines():
        t = row.split('#')[0].split()
        if not t:
            continue
        if t[0] == 'stream':
            streams[t[1]] = dict(capacity=int(t[5]), crossing=int(t[6]), kind=t[2])
        elif t[0] == 'phase':
            phases[t[1]] = set(t[2:])
        elif t[0] == 'conflict':
            conflicts[frozenset(t[1:3])] = int(t[3])
        elif len(t) == 2 and t[0] in settings:
            settings[t[0]] = int(t[1])
    for line in text.splitlines():
        if line.startswith('EVENT '):
            _, seq, now, stream, message = line.split(' ', 4)
            assert int(seq) not in records, line
            records[int(seq)] = int(now), int(stream), message
    assert records and sorted(records) == list(range(1, len(records) + 1))
    queues = {s: [] for s in streams}
    counts = {s: [0, 0, 0] for s in streams}
    active, arrivals, pedestrian = {}, {}, set()
    phase, state, since, cleared = set(), 'initial', 0, None
    last_time, stopped, summary, total_wait, simultaneous = 0, False, None, 0, False
    tick = settings['tick_ms'] * 1000000
    for seq in sorted(records):
        now, stream, message = records[seq]
        assert now >= last_time
        last_time = now
        event = message.split(':', 1)[0]
        fields = dict(re.findall(r'(\w+)=([^ ]+)', message))
        name, ident = fields.get('поток'), int(fields.get('id', '0'))
        if stopped:
            assert event in ('Итог потока', 'ИТОГО')
        if name:
            assert list(streams)[stream] == name
        else:
            assert stream == -1
        if event == 'Начало фазы':
            assert not active and fields['имя'] in phases
            if cleared is not None:
                assert now - cleared >= settings['clearance'] * tick
            phase, state, since = phases[fields['имя']], 'green', now
        elif event == 'Въезд закрыт':
            assert state == 'green' and now - since >= settings['min_green'] * tick
            state = 'drain'
        elif event == 'Конец фазы':
            assert state == 'drain' and not active
        elif event == 'Интервал':
            assert state == 'drain' and not active
            state, cleared = 'clearance', now
        elif event == 'Сигналы':
            assert int(fields['маска']) == sum(1 << i for i, s in enumerate(streams)
                if s in phase and state in ('green', 'drain'))
        elif event == 'Прибытие':
            assert ident not in arrivals
            arrivals[ident] = now
            counts[name][0] += 1
        elif event == 'Очередь':
            assert ident in arrivals and ident not in queues[name]
            queues[name].append(ident)
            assert len(queues[name]) == int(fields['размер']) <= streams[name]['capacity']
            if streams[name]['kind'] == 'pedestrian':
                pedestrian.add(ident)
        elif event == 'Запрос пешехода':
            assert ident in pedestrian
            pedestrian.remove(ident)
        elif event == 'Отказ':
            assert len(queues[name]) == streams[name]['capacity']
            counts[name][2] += 1
        elif event == 'Разрешение':
            assert state == 'green' and name in phase and name not in active
            assert now - since < settings['max_green'] * tick
            assert queues[name].pop(0) == ident and int(fields['очередь']) == len(queues[name])
            for other in active:
                assert conflicts.get(frozenset((name, other)), 1) == 0
            deadline = int(fields['срок'])
            assert deadline == now + streams[name]['crossing'] * tick
            active[name] = ident, deadline, now
            simultaneous |= len(active) > 1
        elif event == 'Выход':
            saved, deadline, entered = active.pop(name)
            assert saved == ident and now >= deadline
            wait = (entered - arrivals[ident]) // tick
            assert int(fields['ожидание']) == wait
            total_wait += wait
            counts[name][1] += 1
        elif event == 'Завершение':
            assert not stopped
            stopped = True
        elif event == 'Итог потока':
            assert [int(fields[x]) for x in ['прибыло', 'прошло', 'отказов']] == counts[name]
            assert int(fields['ожидают']) == len(queues[name])
            assert int(fields['в_зоне']) == int(name in active)
        elif event == 'ИТОГО':
            expected = [sum(c[i] for c in counts.values()) for i in range(3)]
            assert [int(fields[x]) for x in ['прибыло', 'прошло', 'отказов']] == expected
            assert int(fields['ожидают']) == sum(map(len, queues.values()))
            assert int(fields['в_зоне']) == len(active) and int(fields['ожидание']) == total_wait
            assert expected[0] == sum(expected[1:]) + sum(map(len, queues.values())) + len(active)
            summary = dict(passed=expected[1], refused=expected[2], arrived=expected[0],
                           simultaneous=simultaneous)
    assert stopped and summary is not None and not pedestrian
    return summary
