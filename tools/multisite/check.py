"""Strict operation checks. Transport errors are never evidence of absence."""


def check_transaction(request, result, observed):
    assert len(observed) == len(request['keys']), 'incomplete read set'
    present = []
    for item in observed:
        assert 'value' in item or item == {'absent': True}, ('unreadable', item)
        if 'value' in item:
            assert item['value'] == request['value'], ('wrong value', item, request)
        present.append('value' in item)
    assert len(set(present)) <= 1, ('D19 torn transaction', request, observed)
    outcome = result['outcome']
    if outcome == 'ack':
        assert all(present), ('acknowledgement lost after process stop', request, observed)
    elif outcome in ('abort', 'begin_error', 'write_error', 'read_error', 'prepare_error'):
        assert not any(present), ('definite failure committed', request, result, observed)
    elif outcome == 'unknown' or is_transport_error(result):
        pass  # All-present and all-absent are both permitted.
    else:
        raise AssertionError(('D33 definite commit failure', request, result))


TRANSPORT_MARKERS = ('RPCError', 'Rpc', 'Transport', 'Disconnected', 'Timeout', 'ConnectionReset', 'Io(')


def is_transport_error(result):
    """A commit error that names the transport. The client cannot know the outcome."""
    return result.get('outcome') == 'commit_error' and any(m in result.get('error', '') for m in TRANSPORT_MARKERS)


def check_batch(requests, results, observed):
    """Concurrent transactions write the same keys with different values.
    The final state must be one writer's complete value (D19)."""
    keys = requests[0]['keys']
    assert all(r['keys'] == keys for r in requests), 'batch keys differ'
    assert len(observed) == len(keys), 'incomplete read set'
    for item in observed:
        assert 'value' in item or item == {'absent': True}, ('unreadable', item)
    present = ['value' in item for item in observed]
    assert len(set(present)) == 1, ('D19 torn batch', requests[0]['keys'], observed)
    possible = set()
    acked = False
    for request, result in zip(requests, results):
        outcome = result['outcome']
        if outcome == 'ack':
            acked = True
            possible.add(request['value'])
        elif outcome == 'unknown' or is_transport_error(result):
            possible.add(request['value'])
        elif outcome in ('abort', 'begin_error', 'write_error', 'read_error', 'prepare_error'):
            pass
        else:
            raise AssertionError(('D33 definite commit failure', request, result))
    if present[0]:
        values = {item['value'] for item in observed}
        assert len(values) == 1, ('D19 mixed writers', observed)
        assert values <= possible, ('value of a failed writer is visible', values, possible)
    else:
        assert not acked, ('acknowledgement lost', requests[0]['keys'], observed)


def check_protocol_events(events):
    """F1 and F4 from the structured test events of one run. Returns counters."""
    counters = {'status_release': 0, 'status_answer_committed': 0, 'commit_closed_unsynced': 0}
    for event in events:
        detail = event.get('detail')
        if not isinstance(detail, dict):
            continue
        if event['point'] == 'before_status_release':
            counters['status_release'] += 1
            participants = set(detail['participants'])
            assert detail['self_synced'], ('F4 release before own sync', event)
            assert len(set(detail['synced_peers']) & participants) >= len(participants) - 1, ('F4 release before all participants synced', event)
        elif event['point'] == 'status_answer':
            counters['status_answer_committed'] += 1
            assert detail['self_synced'], ('F1 Committed answered from an unsynced record', event)
        elif event['point'] == 'commit_closed' and not detail['synced']:
            counters['commit_closed_unsynced'] += 1
    return counters
