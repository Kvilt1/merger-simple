#!/usr/bin/env python3
"""
Day-first index converter for Snapchat media mapper.
Converts conversation-based output to day-indexed format for efficient browsing.
"""

from __future__ import annotations
import json, re, sys, logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# --- JSON formatting globals ---
JSON_INDENT = 2
JSON_COMPACT = False

# -------------- IO helpers

def read_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_json(obj, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        if JSON_COMPACT:
            json.dump(
                obj, f,
                ensure_ascii=False,
                separators=(",", ":"),   # compact
                sort_keys=True
            )
        else:
            json.dump(
                obj, f,
                ensure_ascii=False,
                indent=JSON_INDENT,      # pretty
                sort_keys=True
            )
            f.write("\n")  # nice trailing newline

# -------------- Time handling

def parse_created_fields(msg: dict):
    """
    Canonical timestamp: field 'Created(microseconds)' is actually *milliseconds*.
    Fallback: parse 'Created' as 'YYYY-MM-DD HH:MM:SS UTC'.

    Returns (t_ms:int, t_iso:str, t_us:int, created_str_iso:str|None, created_str_delta_ms:int|None)
    created_str_iso: ISO derived from "Created" string (if present)
    created_str_delta_ms: abs difference between t_ms and parsed string, in ms (if present)
    """
    t_ms = None
    v = msg.get("Created(microseconds)")
    if isinstance(v, (int, float)):
        t_ms = int(v)
    else:
        s = msg.get("Created")
        if s and isinstance(s, str):
            if s.endswith(" UTC"):
                s = s[:-4]
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            t_ms = int(dt.timestamp() * 1000)
    if t_ms is None:
        # should be rare; use now to avoid crashes but flag in validation
        dt_now = datetime.now(timezone.utc)
        t_ms = int(dt_now.timestamp() * 1000)

    t_iso = datetime.fromtimestamp(t_ms/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")

    created_str_iso = None
    created_str_delta_ms = None
    s2 = msg.get("Created")
    if s2 and isinstance(s2, str):
        s2_raw = s2[:-4] if s2.endswith(" UTC") else s2
        try:
            dt2 = datetime.strptime(s2_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            created_str_iso = dt2.isoformat().replace("+00:00","Z")
            created_str_delta_ms = abs(int(dt2.timestamp()*1000) - t_ms)
        except Exception:
            pass

    return t_ms, t_iso, t_ms*1000, created_str_iso, created_str_delta_ms

# -------------- Misc helpers

_slug_rx = re.compile(r"[\\/*?:\"<>|]")

def slugify(name: str) -> str:
    return _slug_rx.sub("", name)[:120].strip() or "unknown"

def sha1_of_file(p: Path, buf_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with p.open("rb") as f:
        while True:
            b = f.read(buf_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def copy_into_pool(src: Path, pool_dir: Path, use_hash: bool):
    pool_dir.mkdir(parents=True, exist_ok=True)
    if not src.is_file():
        return None
    ext = src.suffix.lower() or ""
    if use_hash:
        h = sha1_of_file(src)
        dest = pool_dir / f"{h}{ext}"
    else:
        dest = pool_dir / src.name
    if not dest.exists():
        shutil.copy2(src, dest)
    return f"/m/{dest.name}"

# -------------- Types

@dataclass
class Event:
    id: str
    t_ms: int
    t_iso: str
    from_user: str | None
    kind: str       # "chat" | "snap" (we default to "chat" unless msg has explicit type)
    media_type: str | None  # Original Media Type from source JSON (e.g., "VIDEO", "IMAGE", "TEXT", etc.)
    text: str | None
    saved: bool
    media: list     # list of {"path": "/m/<file>", "name": "orig.ext"}

# -------------- Input traversal (existing output)

def iter_conversation_jsons(root: Path):
    for top in ("conversations", "groups"):
        base = root / top
        if not base.exists():
            continue
        for conv_dir in sorted(base.iterdir()):
            if not conv_dir.is_dir():
                continue
            cj = conv_dir / "conversation.json"
            if cj.exists():
                yield (top == "groups", conv_dir, cj)

def load_conversation_json(p: Path) -> tuple[dict, list[dict]]:
    j = read_json(p)
    return j.get("conversation_metadata") or {}, j.get("messages") or []

def _title_from_meta(meta: dict) -> str | None:
    # Prefer explicit group name; otherwise a compact synthesized label
    name = meta.get("group_name")
    if name:
        return name
    parts = [p.get("display_name") or p.get("username") for p in (meta.get("participants") or [])]
    parts = [p for p in parts if p]
    if not parts:
        return None
    if len(parts) <= 3:
        return ", ".join(parts)
    return f"{', '.join(parts[:2])} +{len(parts)-2}"

def slim_conv_meta(meta: dict) -> dict:
    conv_id = meta.get("conversation_id")
    ctype = meta.get("conversation_type") or ("group" if meta.get("is_group") else "individual")

    # Keep usernames only; the UI can resolve display names via friends
    participants = []
    for p in (meta.get("participants") or []):
        u = p.get("username") or p.get("display_name")
        if u: participants.append(u)

    return {
        "id": conv_id,
        "type": ctype,                    # "group" | "individual"
        "title": _title_from_meta(meta),  # nice label for headers/sidebars
        "participants": participants      # usernames only (compact)
    }

def day_key_from_ms(t_ms: int) -> tuple[int,int,int,str]:
    dt = datetime.fromtimestamp(t_ms/1000, tz=timezone.utc)
    return dt.year, dt.month, dt.day, dt.strftime("%Y-%m-%d")

# Expand media_locations (files or grouped dirs); skip timestamps.json
def expand_media_files(conv_root: Path, locations: list[str]) -> list[Path]:
    out = []
    for loc in locations or []:
        p = conv_root / loc
        if str(loc).endswith("/"):
            if p.is_dir():
                for child in sorted(p.iterdir()):
                    if child.is_file() and child.name != "timestamps.json":
                        out.append(child)
        else:
            if p.is_file():
                out.append(p)
    return out

# -------------- Conversion

def normalize_message(conv_id: str, msg_idx: int, msg: dict,
                      conv_root: Path, media_pool: Path, use_hash: bool,
                      validation_trace: dict) -> Event:
    t_ms, t_iso, _t_us, created_str_iso, created_str_delta_ms = parse_created_fields(msg)
    from_user = msg.get("From")
    saved = bool(msg.get("IsSaved"))
    text = msg.get("Content")
    kind = "snap" if (msg.get("Type") == "snap") else "chat"
    media_type = msg.get("Media Type")  # Extract Media Type from the message

    # Expand the media set exactly as your current output indicates
    in_media_files = expand_media_files(conv_root, msg.get("media_locations") or [])
    media_items = []
    for f in in_media_files:
        web_path = copy_into_pool(f, media_pool, use_hash)
        if web_path:
            media_items.append({"path": web_path, "name": f.name})

    ev = Event(
        id=f"c:{conv_id}:{msg_idx}",
        t_ms=t_ms,
        t_iso=t_iso,
        from_user=from_user,
        kind=kind,
        media_type=media_type,
        text=text,
        saved=saved,
        media=media_items
    )

    # Track per-message expectations for validation
    validation_trace["events_expected"][ev.id] = {
        "conv": conv_id,
        "t_ms": t_ms,
        "t_iso": t_iso,
        "from": from_user,
        "kind": kind,
        "media_type": media_type,
        "text": text,
        "saved": saved,
        "media_paths": [m["path"] for m in media_items],
        "created_str_iso": created_str_iso,
        "created_str_delta_ms": created_str_delta_ms,
        "had_created_string": created_str_iso is not None
    }
    for m in media_items:
        validation_trace["media_expected"].add(m["path"])
    return ev

def convert_from_memory(conversations: Dict[str, List], 
                       friends_map: Dict[str, Dict],
                       account_owner: str,
                       mappings: Dict[str, Dict],
                       temp_media_dir: Path,
                       output_dir: Path,
                       use_hash: bool = True,
                       max_workers: int = 4,
                       avatars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Convert in-memory conversation data to day-first index format.
    
    Args:
        conversations: Dict of conversation_id -> list of messages
        friends_map: Dict of username -> friend data
        account_owner: Username of account owner
        mappings: Dict of conversation_id -> message_index -> media mappings
        temp_media_dir: Path to temporary directory with processed media
        output_dir: Path to output directory for day-index format
        use_hash: Whether to use hash-based filenames in media pool
        max_workers: Number of worker threads for parallel processing
    
    Returns:
        Dict containing statistics about the conversion
    """
    pool_dir = output_dir / "public" / "m"
    avatar_dir = output_dir / "public" / "a"
    out_data = output_dir / "data"
    out_days = out_data / "days"
    out_convs = out_data / "conversations"
    out_orphans = out_data / "orphans"
    
    # Create output directories
    out_days.mkdir(parents=True, exist_ok=True)
    out_convs.mkdir(parents=True, exist_ok=True)
    out_orphans.mkdir(parents=True, exist_ok=True)
    pool_dir.mkdir(parents=True, exist_ok=True)
    avatar_dir.mkdir(parents=True, exist_ok=True)
    
    # Statistics tracking
    stats = {
        'total_events': 0,
        'total_conversations': 0,
        'total_media': 0,
        'total_days': 0,
        'orphaned_media': 0
    }
    
    validation_trace = {
        "events_expected": {},
        "media_expected": set(),
        "convs_expected": set(),
        "days_expected": set(),
        "orphans_expected_pool": set(),
        "timestamp_string_mismatches": [],
        "messages_without_created": 0
    }
    
    # Build day buckets and conversation metadata
    day_buckets: dict[str, dict] = {}
    conv_meta_written: set[str] = set()
    mapped_files = set()
    
    logger.info("Converting conversations to day-index format...")
    
    # Process each conversation
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        
        for conv_id, messages in conversations.items():
            # Build conversation metadata
            from conversation import create_conversation_metadata, get_conversation_folder_name
            metadata = create_conversation_metadata(conv_id, messages, friends_map, account_owner)
            slim = slim_conv_meta(metadata)
            validation_trace["convs_expected"].add(slim["id"])
            
            def process_conversation(conv_id=conv_id, slim=slim, messages=messages):
                events = []
                conv_mappings = mappings.get(conv_id, {})
                
                for i, msg in enumerate(messages):
                    # Get media files for this message
                    media_locations = []
                    if i in conv_mappings:
                        for item in conv_mappings[i]:
                            media_locations.append(item["filename"])
                            mapped_files.add(item["filename"])
                    
                    # Create normalized event
                    ev = normalize_message_from_memory(
                        conv_id, i, msg, media_locations, 
                        temp_media_dir, pool_dir, use_hash, validation_trace
                    )
                    events.append(ev)
                
                return conv_id, slim, events
            
            futures.append(ex.submit(process_conversation))
        
        # Process results
        for fut in as_completed(futures):
            conv_id, slim, events = fut.result()
            
            # Write conversation metadata
            if conv_id not in conv_meta_written:
                write_json(slim, out_convs / conv_id / "meta.json")
                conv_meta_written.add(conv_id)
                stats['total_conversations'] += 1
            
            # Add events to day buckets
            for ev in events:
                _, _, _, day_str = day_key_from_ms(ev.t_ms)
                validation_trace["days_expected"].add(day_str)
                
                bucket = day_buckets.setdefault(day_str, {
                    "events": [],
                    "conversations": {},
                    "gallery": []
                })
                
                # Record event
                bucket["events"].append({
                    "id": ev.id,
                    "t_ms": ev.t_ms,
                    "t_iso": ev.t_iso,
                    "from": ev.from_user,
                    "kind": ev.kind,
                    "media_type": ev.media_type,
                    "text": ev.text,
                    "saved": ev.saved,
                    "media": ev.media
                })
                stats['total_events'] += 1
                
                # Add conversation to day
                bucket["conversations"][slim["id"]] = slim
                
                # Add media to gallery
                for mitem in ev.media:
                    bucket["gallery"].append({"event": ev.id, "path": mitem["path"]})
                    stats['total_media'] += 1
    
    # Write per-day json files
    logger.info(f"Writing {len(day_buckets)} day index files...")
    all_days = []
    for day_str, bucket in sorted(day_buckets.items()):
        events = sorted(bucket["events"], key=lambda e: e["t_ms"])
        convs = sorted(bucket["conversations"].values(), key=lambda c: (c["type"], c["title"] or ""))
        day_stats = {
            "events": len(events),
            "conversations": len(convs),
            "media": sum(len(e.get("media") or []) for e in events)
        }
        y, m, d = day_str.split("-")
        write_json({
            "date": day_str,
            "events": events,
            "conversations": convs,
            "gallery": bucket["gallery"],
            "stats": day_stats
        }, out_days / y / m / d / "index.json")
        all_days.append(day_str)
    
    stats['total_days'] = len(all_days)
    write_json({"days": all_days}, out_days / "index.json")
    
    # Handle orphaned media
    logger.info("Processing orphaned media...")
    orphan_entries = []
    orphaned_count = 0
    
    for item in temp_media_dir.iterdir():
        # Skip if already mapped, thumbnails, or overlays
        if item.name in mapped_files:
            continue
        if "thumbnail" in item.name.lower():
            continue
        if "_overlay~" in item.name:
            continue
        
        if item.is_file():
            web = copy_into_pool(item, pool_dir, use_hash)
            if web:
                orphan_entries.append({"path": web, "name": item.name})
                validation_trace["orphans_expected_pool"].add(web)
                orphaned_count += 1
        elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
            for child in sorted(item.iterdir()):
                if child.is_file() and child.name != "timestamps.json":
                    web = copy_into_pool(child, pool_dir, use_hash)
                    if web:
                        orphan_entries.append({"path": web, "name": child.name})
                        validation_trace["orphans_expected_pool"].add(web)
                        orphaned_count += 1
    
    stats['orphaned_media'] = orphaned_count
    write_json({"items": orphan_entries}, out_orphans / "index.json")
    
    # Generate avatar pool and friends.min.json
    if avatars:
        logger.info("Generating avatar pool and friends.min.json...")
        from bitmoji_processing import save_avatar_pool
        
        # Save avatars to pool
        username_to_avatar_path = save_avatar_pool(avatars, avatar_dir)
        
        # Build friends.min.json - only include users with avatars
        friends_min = {}
        
        # Process users who have avatars
        for username, avatar_path in username_to_avatar_path.items():
            # Get display name from friends_map if available
            display_name = username
            if username in friends_map:
                friend_data = friends_map[username]
                display_name = friend_data.get("Display Name") or friend_data.get("display_name") or username
            elif username == account_owner:
                display_name = "You"
            
            friends_min[username] = {
                "name": display_name,
                "avatar": avatar_path
            }
        
        # Write friends.min.json
        write_json(friends_min, out_data / "friends.min.json")
        logger.info(f"Generated friends.min.json with {len(friends_min)} users")
        stats['total_friends'] = len(friends_min)
        stats['total_avatars'] = len(username_to_avatar_path)
    
    logger.info(f"Conversion complete: {stats['total_events']} events, {stats['total_media']} media items, {stats['total_days']} days")
    
    return stats

def normalize_message_from_memory(conv_id: str, msg_idx: int, msg: dict,
                                 media_files: List[str], temp_media_dir: Path,
                                 media_pool: Path, use_hash: bool,
                                 validation_trace: dict) -> Event:
    """
    Normalize a message from in-memory data to an Event object.
    Similar to normalize_message but works with in-memory media file list.
    """
    t_ms, t_iso, _t_us, created_str_iso, created_str_delta_ms = parse_created_fields(msg)
    from_user = msg.get("From")
    saved = bool(msg.get("IsSaved"))
    text = msg.get("Content")
    kind = "snap" if (msg.get("Type") == "snap") else "chat"
    media_type = msg.get("Media Type")  # Extract Media Type from the message
    
    # Process media files
    media_items = []
    for filename in media_files:
        # Handle both regular files and grouped folders
        if filename.endswith("_multipart") or filename.endswith("_grouped"):
            # It's a folder - process all files inside
            folder_path = temp_media_dir / filename
            if folder_path.is_dir():
                for child in sorted(folder_path.iterdir()):
                    if child.is_file() and child.name != "timestamps.json":
                        web_path = copy_into_pool(child, media_pool, use_hash)
                        if web_path:
                            media_items.append({"path": web_path, "name": child.name})
        else:
            # Regular file
            file_path = temp_media_dir / filename
            if file_path.is_file():
                web_path = copy_into_pool(file_path, media_pool, use_hash)
                if web_path:
                    media_items.append({"path": web_path, "name": filename})
    
    ev = Event(
        id=f"c:{conv_id}:{msg_idx}",
        t_ms=t_ms,
        t_iso=t_iso,
        from_user=from_user,
        kind=kind,
        media_type=media_type,
        text=text,
        saved=saved,
        media=media_items
    )
    
    # Track for validation
    validation_trace["events_expected"][ev.id] = {
        "conv": conv_id,
        "t_ms": t_ms,
        "t_iso": t_iso,
        "from": from_user,
        "kind": kind,
        "media_type": media_type,
        "text": text,
        "saved": saved,
        "media_paths": [m["path"] for m in media_items],
        "created_str_iso": created_str_iso,
        "created_str_delta_ms": created_str_delta_ms,
        "had_created_string": created_str_iso is not None
    }
    for m in media_items:
        validation_trace["media_expected"].add(m["path"])
    
    return ev

def convert(src: Path, out: Path, use_hash: bool, max_workers: int, do_validate: bool, validate_only: bool):
    pool_dir: Path = out / "public" / "m"
    out_data = out / "data"
    out_days = out_data / "days"
    out_convs = out_data / "conversations"
    out_orphans = out_data / "orphans"

    if not validate_only:
        out_days.mkdir(parents=True, exist_ok=True)
        out_convs.mkdir(parents=True, exist_ok=True)
        out_orphans.mkdir(parents=True, exist_ok=True)
        pool_dir.mkdir(parents=True, exist_ok=True)

    validation_trace = {
        "events_expected": {},   # id -> fields
        "media_expected": set(), # /m/.. paths (from events)
        "convs_expected": set(), # conversation ids
        "days_expected": set(),  # yyyy-mm-dd
        "orphans_expected_pool": set(), # /m/.. paths created from orphaned/
        "timestamp_string_mismatches": [], # (event_id, delta_ms, str_iso, t_iso)
        "messages_without_created": 0
    }

    if not validate_only:
        # Pass 1: process conversations, build day buckets and write slim conv meta
        day_buckets: dict[str, dict] = {}
        conv_meta_written: set[str] = set()

        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for is_group, conv_dir, cj in iter_conversation_jsons(src):
                meta, msgs = load_conversation_json(cj)
                conv_id = meta.get("conversation_id") or slugify(conv_dir.name)
                slim = slim_conv_meta(meta)
                validation_trace["convs_expected"].add(slim["id"])

                def process_one(conv_id=conv_id, slim=slim, msgs=msgs, conv_dir=conv_dir):
                    events = []
                    for i, m in enumerate(msgs):
                        ev = normalize_message(conv_id, i, m, conv_dir, pool_dir, use_hash, validation_trace)
                        events.append(ev)
                    return conv_id, slim, events

                futures.append(ex.submit(process_one))

            for fut in as_completed(futures):
                conv_id, slim, events = fut.result()

                if conv_id not in conv_meta_written:
                    write_json(slim, out_convs / conv_id / "meta.json")
                    conv_meta_written.add(conv_id)

                for ev in events:
                    _, _, _, day_str = day_key_from_ms(ev.t_ms)
                    validation_trace["days_expected"].add(day_str)
                    bucket = day_buckets.setdefault(day_str, {
                        "events": [],
                        "conversations": {},
                        "gallery": []
                    })
                    # record event
                    bucket["events"].append({
                        "id": ev.id,
                        "t_ms": ev.t_ms,
                        "t_iso": ev.t_iso,
                        "from": ev.from_user,
                        "kind": ev.kind,
                        "media_type": ev.media_type,
                        "text": ev.text,
                        "saved": ev.saved,
                        "media": ev.media
                    })
                    # conv meta for the day
                    bucket["conversations"][slim["id"]] = slim
                    # gallery pointers
                    for mitem in ev.media:
                        bucket["gallery"].append({"event": ev.id, "path": mitem["path"]})

        # Write per-day json + global days index
        all_days = []
        for day_str, bucket in sorted(day_buckets.items()):
            events = sorted(bucket["events"], key=lambda e: e["t_ms"])
            convs = sorted(bucket["conversations"].values(), key=lambda c: (c["type"], c["title"] or ""))
            stats = {
                "events": len(events),
                "conversations": len(convs),
                "media": sum(len(e.get("media") or []) for e in events)
            }
            y, m, d = day_str.split("-")
            write_json({
                "date": day_str,
                "events": events,
                "conversations": convs,
                "gallery": bucket["gallery"],
                "stats": stats
            }, out_days / y / m / d / "index.json")
            all_days.append(day_str)
        write_json({"days": all_days}, out_days / "index.json")

        # Orphans -> pool + index
        orphan_src = src / "orphaned"
        orphan_entries = []
        if orphan_src.exists():
            for item in sorted(orphan_src.iterdir()):
                if item.is_file():
                    web = copy_into_pool(item, pool_dir, use_hash)
                    if web:
                        orphan_entries.append({"path": web, "name": item.name})
                        validation_trace["orphans_expected_pool"].add(web)
                elif item.is_dir():
                    for child in sorted(item.iterdir()):
                        if child.is_file() and child.name != "timestamps.json":
                            web = copy_into_pool(child, pool_dir, use_hash)
                            if web:
                                orphan_entries.append({"path": web, "name": child.name})
                                validation_trace["orphans_expected_pool"].add(web)
        write_json({"items": orphan_entries}, out_orphans / "index.json")

    # Always validate unless explicitly skipped
    if do_validate or validate_only:
        ok = validate_all(src, out, validation_trace, validate_only)
        if not ok:
            sys.exit(1)

    if not validate_only:
        print(f"✅ Wrote day-first dataset to: {out}")
        if do_validate:
            print("✅ Validation passed.")

# -------------- Validation

def validate_all(src: Path, out: Path, trace: dict, validate_only: bool) -> bool:
    problems = []

    out_data = out / "data"
    out_days = out_data / "days"
    out_convs = out_data / "conversations"
    out_orphans = out_data / "orphans"
    pool_dir = out / "public" / "m"

    # Build actual event map by scanning days/*
    events_actual = {}
    gallery_actual = []
    days_found = set()
    if not out_days.exists():
        problems.append("days/ not found.")
    else:
        # days index
        try:
            idx = read_json(out_days / "index.json")
            for day in idx.get("days", []):
                days_found.add(day)
        except Exception:
            problems.append("days/index.json unreadable or missing 'days'.")

        # each day subfile
        for day in sorted(days_found):
            y, m, d = day.split("-")
            p = out_days / y / m / d / "index.json"
            if not p.exists():
                problems.append(f"Day file missing: {p}")
                continue
            j = read_json(p)
            if j.get("date") != day:
                problems.append(f"Day mismatch in {p}: {j.get('date')} != {day}")
            for ev in j.get("events", []):
                events_actual[ev["id"]] = ev
                # basic schema sanity
                for k in ("id","t_ms","t_iso","from","kind","saved","media"):
                    if k not in ev:
                        problems.append(f"Event missing field '{k}': {ev.get('id')}")
                if not isinstance(ev.get("media", []), list):
                    problems.append(f"Event.media not a list: {ev.get('id')}")
            for g in j.get("gallery", []):
                gallery_actual.append(g)
                if g.get("event") not in events_actual and not validate_only:
                    # In validate_only we can’t depend on in-memory events; recheck later.
                    pass

    # Conversation metas present?
    conv_metas_found = set()
    if out_convs.exists():
        for conv_id_dir in out_convs.iterdir():
            if (conv_id_dir / "meta.json").exists():
                try:
                    m = read_json(conv_id_dir / "meta.json")
                    if "id" in m:
                        conv_metas_found.add(m["id"])
                except Exception:
                    problems.append(f"Unreadable meta.json: {conv_id_dir}")
    else:
        problems.append("conversations/ not found.")

    # Orphans index present?
    orphans_list = []
    if (out_orphans / "index.json").exists():
        try:
            o = read_json(out_orphans / "index.json")
            orphans_list = [i.get("path") for i in o.get("items", []) if i.get("path")]
        except Exception:
            problems.append("orphans/index.json unreadable.")

    # Pool existence checks (files referenced must exist)
    missing_pool_files = []
    for ev in events_actual.values():
        for m in ev.get("media", []):
            p = m.get("path")
            if p and not (pool_dir / Path(p).name).exists():
                missing_pool_files.append(p)
    for op in orphans_list:
        if op and not (pool_dir / Path(op).name).exists():
            missing_pool_files.append(op)
    if missing_pool_files:
        problems.append(f"Missing media in pool: {min(len(missing_pool_files),10)} shown -> {missing_pool_files[:10]} (and more)")

    # Gallery ↔ events consistency
    gallery_event_missing = [g for g in gallery_actual if g.get("event") not in events_actual]
    if gallery_event_missing:
        problems.append(f"Gallery references unknown event ids: {min(len(gallery_event_missing),10)} shown")

    # If we converted this run (trace has expectations), compare deep
    if trace["events_expected"]:
        # 1) every expected event exists exactly once
        missing_events = [eid for eid in trace["events_expected"].keys() if eid not in events_actual]
        if missing_events:
            problems.append(f"Missing events in output: {min(len(missing_events),10)} shown -> {missing_events[:10]} (and more)")

        # 2) payload parity (core fields + media list equality)
        field_mismatches = []
        media_mismatches = []
        ts_string_mismatches = []
        for eid, exp in trace["events_expected"].items():
            act = events_actual.get(eid)
            if not act:
                continue
            # Core fields
            core = ("t_ms","t_iso","from","kind","text","saved")
            for k in core:
                if act.get(k) != exp.get(k):
                    field_mismatches.append((eid, k, exp.get(k), act.get(k)))
            # Media list (as set of paths)
            exp_paths = sorted(exp["media_paths"])
            act_paths = sorted([m.get("path") for m in act.get("media", []) if m.get("path")])
            if exp_paths != act_paths:
                media_mismatches.append((eid, exp_paths, act_paths))
            # Timestamp cross-check vs Created string
            if exp["had_created_string"]:
                delta = exp.get("created_str_delta_ms")
                if delta is None or delta > 1000:
                    ts_string_mismatches.append((eid, delta, exp.get("created_str_iso"), exp.get("t_iso")))
        if field_mismatches:
            problems.append(f"Event field mismatches: {min(len(field_mismatches),5)} shown -> {field_mismatches[:5]} (and more)")
        if media_mismatches:
            problems.append(f"Event media mismatches: {min(len(media_mismatches),5)} shown")
        if ts_string_mismatches:
            problems.append(f"Timestamps differ from 'Created' string by >1s: {min(len(ts_string_mismatches),10)} shown")

        # 3) gallery size per day == sum media
        per_day_gallery_prob = []
        # Build per-day stats from actual files
        for day in days_found:
            y,m,d = day.split("-")
            j = read_json(out_data / "days" / y / m / d / "index.json")
            media_count = sum(len(e.get("media") or []) for e in j.get("events", []))
            gal_count = len(j.get("gallery", []))
            if gal_count != media_count:
                per_day_gallery_prob.append((day, media_count, gal_count))
        if per_day_gallery_prob:
            problems.append(f"Gallery count mismatches: {per_day_gallery_prob[:10]}")

        # 4) conversations meta presence
        conv_expected = trace["convs_expected"]
        missing_conv_meta = [c for c in conv_expected if c not in conv_metas_found]
        if missing_conv_meta:
            problems.append(f"Missing conversations meta.json for: {missing_conv_meta[:10]}")

        # 5) orphans present in index and pool
        orphan_missing_in_index = []
        if trace["orphans_expected_pool"]:
            idx_set = set(orphans_list)
            for p in trace["orphans_expected_pool"]:
                if p not in idx_set:
                    orphan_missing_in_index.append(p)
        if orphan_missing_in_index:
            problems.append(f"Orphans missing in index.json: {orphan_missing_in_index[:10]}")

    # Create a validation report
    report = {
        "ok": not problems,
        "problems": problems,
        "summary": {
            "events_actual": len(events_actual),
            "gallery_items": len(gallery_actual),
            "days_found": len(days_found),
            "conversations_meta_found": len(conv_metas_found),
            "orphans_listed": len(orphans_list)
        }
    }
    write_json(report, out / "validation_report.json")

    if problems:
        print("❌ VALIDATION FAILED. See validation_report.json for details.")
        for p in problems[:10]:
            print(" -", p)
        if len(problems) > 10:
            print(f"   ... and {len(problems)-10} more")
        return False

    print("✅ Validation passed. (validation_report.json written)")
    return True

# -------------- CLI

def main():
    global JSON_INDENT, JSON_COMPACT

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", type=Path, required=True,
                    help="Path to existing output (has conversations/, groups/, orphaned/)")
    ap.add_argument("--out", dest="dst", type=Path, required=True,
                    help="Path to write/read the day-first dataset")
    ap.add_argument("--no-hash", action="store_true",
                    help="Keep original filenames in public/m (no dedupe)")
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--no-validate", action="store_true",
                    help="Skip validation step")
    ap.add_argument("--validate-only", action="store_true",
                    help="Do not convert; just validate current --out against --in")

    # NEW:
    ap.add_argument("--indent", type=int, default=2,
                    help="Spaces for JSON indentation (default: 2)")
    ap.add_argument("--compact", action="store_true",
                    help="Write compact JSON (overrides --indent)")

    args = ap.parse_args()

    JSON_INDENT = max(0, args.indent)
    JSON_COMPACT = bool(args.compact)

    if not args.src.exists():
        print(f"--in not found: {args.src}")
        sys.exit(1)
    if not args.dst.exists():
        # allow creation when not validate-only
        if args.validate_only:
            print(f"--out not found (needed for --validate-only): {args.dst}")
            sys.exit(1)

    convert(
        src=args.src,
        out=args.dst,
        use_hash=not args.no_hash,
        max_workers=args.max_workers,
        do_validate=(not args.no_validate),
        validate_only=args.validate_only
    )

if __name__ == "__main__":
    main()