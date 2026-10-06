"""Drop a JSONL file in small batches to exercise the file streaming job."""
import argparse
import json
import time
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("input", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--batch-size", type=int, default=5)
parser.add_argument("--interval", type=float, default=10)
args = parser.parse_args()

args.output.mkdir(parents=True, exist_ok=True)
with args.input.open(encoding="utf-8") as source:
    batch = []
    batch_number = 0
    for line in source:
        if not line.strip():
            continue
        batch.append(json.loads(line))
        if len(batch) < args.batch_size:
            continue
        target = args.output / f"events_{batch_number:05d}.jsonl"
        target.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in batch), encoding="utf-8")
        print(f"wrote {target} ({len(batch)} events)", flush=True)
        batch = []
        batch_number += 1
        time.sleep(args.interval)
    if batch:
        target = args.output / f"events_{batch_number:05d}.jsonl"
        target.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in batch), encoding="utf-8")
        print(f"wrote {target} ({len(batch)} events)", flush=True)
