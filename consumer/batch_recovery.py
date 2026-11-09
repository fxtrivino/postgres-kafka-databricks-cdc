
import json
from pathlib import Path

def split_contiguous_events(
    events: list[dict]
) -> list[list[dict]]:
    if not events:
        return []

    segments = []
    current = [events[0]]

    for event in events[1:]:
        previous = current[-1]

        if (
            event["topic"] == previous["topic"]
            and event["partition"] == previous["partition"]
            and event["offset"] == previous["offset"] + 1
        ):
            current.append(event)
        else:
            segments.append(current)
            current = [event]

    segments.append(current)

    return segments

def load_published_events(
    output_dir: str,
    topic: str,
    partition: int
) -> dict[int, dict]:
    published = {}

    directory = Path(output_dir)

    for path in sorted(directory.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)

                if (
                    event["topic"] != topic
                    or event["partition"] != partition
                ):
                    continue

                offset = event["offset"]

                if offset in published:
                    if published[offset] != event:
                        raise RuntimeError(
                            f"Conflicting published event "
                            f"at offset {offset}"
                        )
                else:
                    published[offset] = event

    return published


def recover_events(
    events: list[dict],
    output_dir: str
) -> tuple[list[dict], int]:

    if not events:
        return [], 0

    topic = events[0]["topic"]
    partition = events[0]["partition"]

    published = load_published_events(
        output_dir,
        topic,
        partition
    )

    new_events = []
    recovered_count = 0

    for event in events:
        offset = event["offset"]

        if offset in published:
            if published[offset] != event:
                raise RuntimeError(
                    f"Integrity mismatch at offset {offset}"
                )

            recovered_count += 1
        else:
            new_events.append(event)

    return new_events, recovered_count
