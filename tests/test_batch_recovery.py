
from consumer.batch_writer import write_batch
from consumer.batch_recovery import recover_events
from consumer.batch_recovery import split_contiguous_events


def make_event(offset):
    return {
        "topic": "postgres.public.customers",
        "partition": 0,
        "offset": offset,
        "value": {"payload": {"op": "c"}},
        "is_tombstone": False
    }


def test_partial_overlap(tmp_path):
    existing = [
        make_event(7),
        make_event(8),
        make_event(9)
    ]

    write_batch(existing, str(tmp_path))

    incoming = [
        make_event(8),
        make_event(9),
        make_event(10)
    ]

    new_events, recovered = recover_events(
        incoming,
        str(tmp_path)
    )

    assert recovered == 2
    assert len(new_events) == 1
    assert new_events[0]["offset"] == 10


def test_full_recovery(tmp_path):
    events = [
        make_event(7),
        make_event(8)
    ]

    write_batch(events, str(tmp_path))

    new_events, recovered = recover_events(
        events,
        str(tmp_path)
    )

    assert recovered == 2
    assert new_events == []

def test_non_contiguous_recovery(tmp_path):
    write_batch(
        [make_event(7)],
        str(tmp_path)
    )

    write_batch(
        [make_event(9)],
        str(tmp_path)
    )

    incoming = [
        make_event(7),
        make_event(8),
        make_event(9),
        make_event(10)
    ]

    new_events, recovered = recover_events(
        incoming,
        str(tmp_path)
    )

    segments = split_contiguous_events(new_events)

    assert recovered == 2
    assert len(segments) == 2

    assert [e["offset"] for e in segments[0]] == [8]
    assert [e["offset"] for e in segments[1]] == [10]

    for segment in segments:
        write_batch(segment, str(tmp_path))