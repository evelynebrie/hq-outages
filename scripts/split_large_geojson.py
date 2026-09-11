#!/usr/bin/env python3
"""Split oversized GeoJSON files so the site can be pushed to GitHub.

GitHub rejects any file over 100 MB, and the cumulative regional files
(all_datetimes grows every 15 minutes) keep crossing it. Every
FeatureCollection in PUBLIC_DIR larger than --max-mb is rewritten as
<name>.part01.geojson, <name>.part02.geojson, ... Features are kept in
their original order and are never modified: concatenating the parts'
`features` arrays gives back the original file, which this script checks
before deleting the original.

The mapping from each original path to its parts is written to
PUBLIC_DIR/geojson_parts.json, which the site uses to fetch and merge them.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

MANIFEST_NAME = "geojson_parts.json"


def dump(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def split_file(path, max_bytes):
    with open(path, encoding="utf-8") as f:
        original = json.load(f)
    if original.get("type") != "FeatureCollection":
        sys.exit(f"{path}: over the limit but not a FeatureCollection")

    header = {k: v for k, v in original.items() if k != "features"}
    header_bytes = len(dump({**header, "features": []}).encode("utf-8"))

    # Greedy packing in original order, sized on the exact bytes written.
    chunks, current, current_bytes = [], [], header_bytes
    for feature in original["features"]:
        size = len(dump(feature).encode("utf-8")) + 1  # +1 for the comma
        if header_bytes + size > max_bytes:
            sys.exit(f"{path}: a single feature is larger than {max_bytes} bytes")
        if current and current_bytes + size > max_bytes:
            chunks.append(current)
            current, current_bytes = [], header_bytes
        current.append(feature)
        current_bytes += size
    chunks.append(current)

    for stale in path.parent.glob(f"{path.stem}.part*.geojson"):
        stale.unlink()
    parts = []
    for i, features in enumerate(chunks, start=1):
        part_path = path.with_name(f"{path.stem}.part{i:02d}.geojson")
        data = dump({**header, "features": features}).encode("utf-8")
        part_path.write_bytes(data)
        parts.append((part_path, hashlib.sha1(data).hexdigest()[:12]))

    # Re-read what was written and confirm nothing was lost or altered.
    rebuilt = []
    for part_path, _ in parts:
        with open(part_path, encoding="utf-8") as f:
            part = json.load(f)
        if {k: v for k, v in part.items() if k != "features"} != header:
            sys.exit(f"{part_path}: header differs from {path}")
        rebuilt.extend(part["features"])
    if rebuilt != original["features"]:
        sys.exit(f"{path}: parts do not reproduce the original features")

    path.unlink()
    return parts, len(original["features"])


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("public_dir", type=Path)
    parser.add_argument("--max-mb", type=float, default=45,
                        help="largest file to leave whole, in MiB (default 45, "
                             "under GitHub's 50 MB warning)")
    args = parser.parse_args()
    max_bytes = int(args.max_mb * 1024 * 1024)
    public_dir = args.public_dir

    # Keep entries from an earlier run whose original is still split, so
    # running the script twice on the same directory is harmless.
    manifest = {}
    manifest_path = public_dir / MANIFEST_NAME
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
        manifest = {
            rel: parts for rel, parts in previous.items()
            if not (public_dir / rel).exists()
            and all((public_dir / p["path"]).exists() for p in parts)
        }

    for path in sorted(public_dir.rglob("*.geojson")):
        if re.search(r"\.part\d+$", path.stem) or path.stat().st_size <= max_bytes:
            continue
        size_mb = path.stat().st_size / 1024 / 1024
        parts, n_features = split_file(path, max_bytes)
        rel = path.relative_to(public_dir).as_posix()
        manifest[rel] = [
            {"path": p.relative_to(public_dir).as_posix(), "sha1": sha}
            for p, sha in parts
        ]
        print(f"  {rel}: {size_mb:.1f} MB, {n_features} features -> {len(parts)} parts "
              f"({', '.join(f'{p.stat().st_size / 1024 / 1024:.1f}' for p, _ in parts)} MB)")

    manifest_path.write_text(
        json.dumps({"max_bytes": max_bytes, "files": manifest}, indent=1) + "\n",
        encoding="utf-8",
    )
    print(f"{len(manifest)} file(s) split; manifest written to {MANIFEST_NAME}")


if __name__ == "__main__":
    main()
