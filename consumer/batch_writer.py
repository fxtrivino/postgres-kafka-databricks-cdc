
import json
import os
import tempfile
from pathlib import Path


def verify_existing_batch(
    target: Path,
    expected_events: list[dict]
) -> bool:
    try:
        with target.open("r", encoding="utf-8") as file:
            existing_events = [
                json.loads(line)
                for line in file
            ]

        return existing_events == expected_events

    except (OSError, ValueError, TypeError):
        return False

def write_batch(events: list[dict], output_dir: str) -> Path:
    if not events:
        raise ValueError("Cannot write an empty batch")

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    # Identidad determinista basada en offsets Kafka.
    first = events[0]
    last = events[-1]

    if any(
        e["topic"] != first["topic"]
        or e["partition"] != first["partition"]
        for e in events
    ):
        raise ValueError("Batch must contain one topic-partition")

    offsets = [e["offset"] for e in events]

    if offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise ValueError("Offsets must be consecutive")

    filename = (
        f"cdc_p{first['partition']}_"
        f"{first['offset']:020d}_"
        f"{last['offset']:020d}.jsonl"
    )

    target = directory / filename

    if target.exists():
        if verify_existing_batch(target, events):
            print(f"Recovered existing batch: {target}")
            return target

        raise RuntimeError(
            f"Batch integrity mismatch: {target}"
        )

    fd, staging = tempfile.mkstemp(
        prefix=".cdc_",
        suffix=".tmp",
        dir=directory
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            for event in events:
                file.write(
                    json.dumps(event, ensure_ascii=False) + "\n"
                )

            file.flush()
            os.fsync(file.fileno())

        # Publicación sin sobrescribir un batch existente.
        try:
            os.link(staging, target)

        except FileExistsError:
            if verify_existing_batch(target, events):
                print(f"Recovered existing batch: {target}")
                return target

            raise RuntimeError(
                f"Batch integrity mismatch: {target}"
            )

        # Sincronizar la entrada de directorio.
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

        return target

    finally:
        if os.path.exists(staging):
            os.unlink(staging)
