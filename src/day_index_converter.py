#!/usr/bin/env python3
"""Simplified day-index converter for Snapchat media mapper."""

import json
import hashlib
import logging
import shutil
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def write_json(data: Any, path: Path) -> None:
    """Write JSON with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)

def parse_timestamp(msg: Dict) -> tuple[int, str, bool]:
    """Parse message timestamp. Returns (ms, iso_string, is_sender)."""
    # Try milliseconds field first
    t_ms = msg.get("Created(microseconds)")
    if t_ms:
        t_ms = int(t_ms)
    else:
        # Parse from Created string
        created = msg.get("Created", "")
        if created.endswith(" UTC"):
            created = created[:-4]
        try:
            dt = datetime.strptime(created, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            t_ms = int(dt.timestamp() * 1000)
        except:
            t_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Convert to ISO format
    t_iso = datetime.fromtimestamp(t_ms/1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    is_sender = bool(msg.get("IsSender"))
    
    return t_ms, t_iso, is_sender

def copy_media_to_pool(src: Path, pool_dir: Path, use_hash: bool = True) -> Optional[str]:
    """Copy media file to pool directory."""
    if not src.is_file():
        return None
    
    pool_dir.mkdir(parents=True, exist_ok=True)
    
    if use_hash:
        # Hash-based filename for deduplication
        h = hashlib.sha1()
        with open(src, 'rb') as f:
            while chunk := f.read(8192):
                h.update(chunk)
        filename = h.hexdigest() + src.suffix.lower()
    else:
        filename = src.name
    
    dest = pool_dir / filename
    if not dest.exists():
        shutil.copy2(src, dest)
    
    return f"/m/{filename}"

def process_media_files(media_locations: List[str], temp_dir: Path, pool_dir: Path, 
                       use_hash: bool = True) -> List[str]:
    """Process media files and return pool paths."""
    pool_paths = []
    
    for location in media_locations:
        path = temp_dir / location
        
        # Handle grouped folders
        if location.endswith(("_multipart", "_grouped")):
            if path.is_dir():
                for child in sorted(path.iterdir()):
                    if child.is_file() and child.name != "timestamps.json":
                        if pool_path := copy_media_to_pool(child, pool_dir, use_hash):
                            pool_paths.append(pool_path)
        # Handle regular files
        elif path.is_file():
            if pool_path := copy_media_to_pool(path, pool_dir, use_hash):
                pool_paths.append(pool_path)
    
    return pool_paths

def convert_from_memory(conversations: Dict[str, List],
                       friends_map: Dict[str, Dict],
                       account_owner: str,
                       mappings: Dict[str, Dict],
                       temp_media_dir: Path,
                       output_dir: Path,
                       use_hash: bool = True,
                       avatars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Convert conversations to simplified day-index format."""
    
    # Setup directories
    pool_dir = output_dir / "public" / "m"
    avatar_dir = output_dir / "public" / "a"
    data_dir = output_dir / "data"
    days_dir = data_dir / "days"
    
    pool_dir.mkdir(parents=True, exist_ok=True)
    avatar_dir.mkdir(parents=True, exist_ok=True)
    days_dir.mkdir(parents=True, exist_ok=True)
    
    # Track statistics
    stats = defaultdict(int)
    
    # Process avatars first
    avatar_paths = {}
    if avatars:
        from bitmoji_processing import save_avatar_pool
        avatar_paths = save_avatar_pool(avatars, avatar_dir)
        stats['total_avatars'] = len(avatar_paths)
    
    # Build master index data
    users = {}
    groups = {}
    all_conversations = {}
    user_conversations = defaultdict(set)
    date_range = {"start": None, "end": None}
    
    # Group messages by day
    day_data = defaultdict(lambda: defaultdict(list))
    mapped_media = set()
    
    logger.info("Processing conversations...")
    
    for conv_id, messages in conversations.items():
        if not messages:
            continue
        
        # Determine conversation type
        is_group = any(msg.get("Conversation Title") for msg in messages)
        
        # Get participants
        participants = set()
        group_title = None
        
        # Always include the owner
        participants.add(account_owner)
        
        for msg in messages:
            if sender := msg.get("From"):
                participants.add(sender)
            if title := msg.get("Conversation Title"):
                group_title = title
        
        # Add conversation ID for individual chats
        if not is_group:
            participants.add(conv_id)
        
        # Update conversation info
        if is_group and group_title:
            groups[conv_id] = {
                "title": group_title,
                "participants": sorted(participants)
            }
            all_conversations[conv_id] = {
                "type": "group",
                "participants": sorted(participants)
            }
        else:
            all_conversations[conv_id] = {
                "type": "individual", 
                "participants": sorted(participants)
            }
        
        # Track user conversations
        for user in participants:
            user_conversations[user].add(conv_id)
        
        # Process messages by day
        for msg_idx, msg in enumerate(messages):
            t_ms, t_iso, is_sender = parse_timestamp(msg)
            
            # Update date range
            date_str = t_iso.split("T")[0]
            if not date_range["start"] or date_str < date_range["start"]:
                date_range["start"] = date_str
            if not date_range["end"] or date_str > date_range["end"]:
                date_range["end"] = date_str
            
            # Get media for this message
            media_files = []
            if conv_id in mappings and msg_idx in mappings[conv_id]:
                for item in mappings[conv_id][msg_idx]:
                    media_files.append(item["filename"])
                    mapped_media.add(item["filename"])
            
            # Process media to pool
            pool_paths = process_media_files(media_files, temp_media_dir, pool_dir, use_hash)
            
            # Store message data
            message_data = {
                "id": f"{conv_id}:{msg_idx}",
                "timestamp": t_ms,
                "timestamp_iso": t_iso,
                "from": msg.get("From"),
                "is_sender": is_sender,
                "kind": "snap" if msg.get("Type") == "snap" else "chat",
                "media_type": msg.get("Media Type"),
                "text": msg.get("Content"),
                "saved": bool(msg.get("IsSaved")),
                "media": pool_paths
            }
            
            day_data[date_str][conv_id].append(message_data)
            stats['total_events'] += 1
            stats['total_media'] += len(pool_paths)
    
    # Build user index with avatars and display names - only for users in conversations
    for username in user_conversations.keys():
        friend_data = friends_map.get(username, {})
        users[username] = {
            "display_name": friend_data.get("Display Name", username),
            "avatar": avatar_paths.get(username, ""),
            "conversations": sorted(user_conversations[username])
        }
    
    # Write master index
    master_index = {
        "owner": account_owner,
        "date_range": date_range,
        "users": users,
        "groups": groups,
        "conversations": all_conversations
    }
    write_json(master_index, data_dir / "index.json")
    
    # Process orphaned media
    logger.info("Processing orphaned media...")
    orphaned_by_day = defaultdict(list)
    
    for item in temp_media_dir.iterdir():
        # Skip mapped files, thumbnails, and overlays
        if item.name in mapped_media or "thumbnail" in item.name.lower() or "_overlay~" in item.name:
            continue
        
        # Extract date from filename (format: YYYY-MM-DD at start of filename)
        date_str = None
        if item.name[:10].count('-') == 2:  # Check if starts with date format
            try:
                potential_date = item.name[:10]
                # Validate it's a real date
                datetime.strptime(potential_date, "%Y-%m-%d")
                date_str = potential_date
            except ValueError:
                pass
        
        # Copy orphaned media to pool
        if item.is_file():
            if pool_path := copy_media_to_pool(item, pool_dir, use_hash):
                # Use extracted date or fall back to first day
                target_date = date_str if date_str else date_range["start"]
                if target_date:
                    orphaned_by_day[target_date].append(pool_path)
                    stats['orphaned_media'] += 1
        elif item.is_dir() and item.name.endswith(("_multipart", "_grouped")):
            # Extract date from folder name
            folder_date = None
            if item.name[:10].count('-') == 2:
                try:
                    potential_date = item.name[:10]
                    datetime.strptime(potential_date, "%Y-%m-%d")
                    folder_date = potential_date
                except ValueError:
                    pass
            
            for child in item.iterdir():
                if child.is_file() and child.name != "timestamps.json":
                    if pool_path := copy_media_to_pool(child, pool_dir, use_hash):
                        # Try to get date from child filename first, then folder, then fallback
                        child_date = None
                        if child.name[:10].count('-') == 2:
                            try:
                                potential_date = child.name[:10]
                                datetime.strptime(potential_date, "%Y-%m-%d")
                                child_date = potential_date
                            except ValueError:
                                pass
                        
                        target_date = child_date or folder_date or date_range["start"]
                        if target_date:
                            orphaned_by_day[target_date].append(pool_path)
                            stats['orphaned_media'] += 1
    
    # Write day indexes and messages
    logger.info(f"Writing {len(day_data)} day indexes...")
    
    for date_str in sorted(day_data.keys()):
        day_dir = days_dir / date_str
        day_dir.mkdir(exist_ok=True)
        
        day_conversations = {}
        
        for conv_id, messages in day_data[date_str].items():
            if not messages:
                continue
            
            # Sort messages by timestamp
            messages.sort(key=lambda m: m['timestamp'])
            
            # Get latest message info
            latest = messages[-1]
            conv_info = all_conversations[conv_id]
            
            # Build conversation summary
            conv_summary = {
                "type": conv_info["type"],
                "latest_timestamp": latest["timestamp"],
                "latest_kind": latest["kind"],
                "is_sender": latest["is_sender"],
                "messages_path": f"messages-{conv_id}/messages.json",
                "media_path": f"messages-{conv_id}/media_mappings.json"
            }
            
            # Add media_type if latest message is a snap
            if latest["kind"] == "snap" and latest.get("media_type"):
                conv_summary["latest_media_type"] = latest["media_type"]
            
            if conv_info["type"] == "group":
                conv_summary["title"] = groups.get(conv_id, {}).get("title", "Unknown Group")
                conv_summary["participants"] = conv_info["participants"]
                if not latest["is_sender"] and latest["from"]:
                    conv_summary["latest_from"] = latest["from"]
            
            day_conversations[conv_id] = conv_summary
            
            # Write conversation messages
            conv_dir = day_dir / f"messages-{conv_id}"
            conv_dir.mkdir(exist_ok=True)
            
            # Prepare messages for output (keep media in both places)
            output_messages = []
            media_mappings = {}
            
            for msg in messages:
                msg_copy = msg.copy()
                media_files = msg_copy.get("media", [])  # Keep media in message
                output_messages.append(msg_copy)
                
                if media_files:
                    media_mappings[msg["id"]] = media_files
            
            write_json(output_messages, conv_dir / "messages.json")
            write_json(media_mappings, conv_dir / "media_mappings.json")
        
        # Write day index
        day_index = {
            "date": date_str,
            "conversations": day_conversations,
            "orphaned_media": orphaned_by_day.get(date_str, [])
        }
        write_json(day_index, day_dir / "index.json")
        stats['total_days'] += 1
    
    stats['total_conversations'] = len(all_conversations)
    
    logger.info(f"Conversion complete: {stats['total_events']} events, "
                f"{stats['total_media']} media items, {stats['total_days']} days")
    
    return dict(stats)