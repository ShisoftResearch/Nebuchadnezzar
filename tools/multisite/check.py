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
    elif outcome == 'unknown':
        pass  # All-present and all-absent are both permitted.
    else:
        raise AssertionError(('D33 definite commit failure', request, result))
