"""Media processing: overlay merging, indexing, and mapping."""

import hashlib
import json
import logging
import os
import re
import shutil
import struct
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

from config import (
    TIMESTAMP_THRESHOLD_SECONDS,
    QUICKTIME_EPOCH_ADJUSTER,
    ensure_directory,
    format_timestamp
)

logger = logging.getLogger(__name__)

def run_ffmpeg_merge(media_file: Path, overlay_file: Path, output_file: Path) -> bool:
    """Merge media with overlay using FFmpeg."""
    if not shutil.which("ffmpeg"):
        return False
    
    try:
        command = [
            "ffmpeg", "-y",
            "-i", str(media_file),
            "-i", str(overlay_file),
            "-filter_complex",
            "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
            "-map", "[vout]",
            "-map", "0:a?",
            "-map_metadata", "0",
            "-movflags", "+faststart",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "18",
            "-c:a", "copy",
            str(output_file)
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            # Preserve timestamps
            st = media_file.stat()
            os.utime(output_file, (st.st_atime, st.st_mtime))
            return True
    except Exception as e:
        logger.error(f"FFmpeg error for {media_file.name}: {e}")
    return False

def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calculate MD5 hash of file."""
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return None

def merge_overlay_pairs(source_dir: Path, temp_dir: Path) -> Set[str]:
    """Find and merge media/overlay pairs."""
    logger.info("Starting overlay merging process")
    
    if not shutil.which("ffmpeg"):
        logger.warning("FFmpeg not found. Skipping overlay merging.")
        return set()
    
    ensure_directory(temp_dir)
    
    # Group files by date
    files_by_date = defaultdict(lambda: {"media": [], "overlay": []})
    for file_path in source_dir.iterdir():
        if not file_path.is_file():
            continue
        
        match = re.match(r"(\d{4}-\d{2}-\d{2})", file_path.name)
        if not match:
            continue
        
        date_str = match.group(1)
        name_lower = file_path.name.lower()
        
        # Skip thumbnails and stubs
        if "thumbnail" in name_lower or "media~zip-" in file_path.name:
            continue
        
        if "_media~" in file_path.name:
            files_by_date[date_str]["media"].append(file_path)
        elif "_overlay~" in file_path.name:
            files_by_date[date_str]["overlay"].append(file_path)
    
    merged_files = set()
    merged_count = 0
    
    for date_str, files in files_by_date.items():
        media_files = files["media"]
        overlay_files = files["overlay"]
        
        # Simple pair
        if len(media_files) == 1 and len(overlay_files) == 1:
            output_file = temp_dir / media_files[0].name
            if run_ffmpeg_merge(media_files[0], overlay_files[0], output_file):
                merged_files.add(media_files[0].name)
                merged_files.add(overlay_files[0].name)
                merged_count += 1
        
        # Multi-part with identical overlays
        elif len(overlay_files) > 1 and len(media_files) == len(overlay_files):
            # Check if overlays identical
            hashes = [calculate_file_hash(f) for f in overlay_files]
            if len(set(h for h in hashes if h)) == 1:
                folder_name = process_multipart(date_str, media_files, overlay_files[0], temp_dir)
                if folder_name:
                    for f in media_files + overlay_files:
                        merged_files.add(f.name)
                    merged_count += 1
            else:
                # Process groups with different overlays
                result = process_grouped_overlays(date_str, media_files, overlay_files, temp_dir)
                merged_files.update(result)
                merged_count += len(result) // 2  # Rough count
        
        # Mismatched counts - try grouping
        elif len(overlay_files) > 1 and len(media_files) > 1:
            result = process_grouped_overlays(date_str, media_files, overlay_files, temp_dir)
            merged_files.update(result)
            merged_count += len(result) // 2
    
    logger.info(f"Successfully merged {merged_count} overlay pairs/groups")
    return merged_files

def process_multipart(date_str: str, media_files: List[Path], overlay_file: Path, temp_dir: Path) -> Optional[str]:
    """Process multi-part video with identical overlay."""
    media_files_sorted = sorted(media_files, key=lambda x: x.name)
    folder_name = media_files_sorted[-1].stem + "_multipart"
    folder_path = temp_dir / folder_name
    
    try:
        ensure_directory(folder_path)
        timestamps = {}
        successful = 0
        
        for media_file in media_files_sorted:
            output_path = folder_path / media_file.name
            if run_ffmpeg_merge(media_file, overlay_file, output_path):
                successful += 1
                timestamp = extract_mp4_timestamp(media_file)
                if timestamp:
                    timestamps[media_file.name] = format_timestamp(timestamp)
        
        if successful > 0:
            with open(folder_path / "timestamps.json", 'w') as f:
                json.dump(timestamps, f, indent=2)
            logger.info(f"Created multi-part folder: {folder_name}")
            return folder_name
    except Exception as e:
        logger.error(f"Error processing multi-part for {date_str}: {e}")
    
    if folder_path.exists():
        shutil.rmtree(folder_path)
    return None

def process_grouped_overlays(date_str: str, media_files: List[Path], overlay_files: List[Path], temp_dir: Path) -> Set[str]:
    """Process groups of overlays and media files."""
    merged = set()
    
    # Group overlays by hash
    overlay_groups = defaultdict(list)
    for overlay in overlay_files:
        file_hash = calculate_file_hash(overlay)
        if file_hash:
            overlay_groups[file_hash].append(overlay)
    
    # Group media by timestamp
    media_with_ts = []
    for media in media_files:
        ts = extract_mp4_timestamp(media)
        if ts:
            media_with_ts.append((media, ts))
    
    media_with_ts.sort(key=lambda x: x[1])
    media_groups = []
    
    if media_with_ts:
        current_group = [media_with_ts[0][0]]
        current_ts = media_with_ts[0][1]
        
        for media, ts in media_with_ts[1:]:
            if abs(ts - current_ts) <= TIMESTAMP_THRESHOLD_SECONDS * 1000:
                current_group.append(media)
            else:
                media_groups.append(current_group)
                current_group = [media]
                current_ts = ts
        
        if current_group:
            media_groups.append(current_group)
    
    # Match groups by count
    overlay_list = list(overlay_groups.values())
    
    for media_group in media_groups:
        for i, overlay_group in enumerate(overlay_list):
            if len(media_group) == len(overlay_group):
                if len(media_group) == 1:
                    # Single pair
                    output = temp_dir / media_group[0].name
                    if run_ffmpeg_merge(media_group[0], overlay_group[0], output):
                        merged.add(media_group[0].name)
                        merged.add(overlay_group[0].name)
                else:
                    # Multi-file group
                    folder_name = create_grouped_folder(media_group, overlay_group, temp_dir)
                    if folder_name:
                        for f in media_group + overlay_group:
                            merged.add(f.name)
                
                overlay_list.pop(i)
                break
    
    return merged

def create_grouped_folder(media_files: List[Path], overlay_files: List[Path], temp_dir: Path) -> Optional[str]:
    """Create folder for grouped media/overlay pairs."""
    media_sorted = sorted(media_files, key=lambda x: x.name)
    overlay_sorted = sorted(overlay_files, key=lambda x: x.name)
    
    folder_name = media_sorted[-1].stem + "_grouped"
    folder_path = temp_dir / folder_name
    
    try:
        ensure_directory(folder_path)
        timestamps = {}
        successful = 0
        
        for media, overlay in zip(media_sorted, overlay_sorted):
            output_path = folder_path / media.name
            if run_ffmpeg_merge(media, overlay, output_path):
                successful += 1
                timestamp = extract_mp4_timestamp(media)
                if timestamp:
                    timestamps[media.name] = format_timestamp(timestamp)
        
        if successful > 0:
            with open(folder_path / "timestamps.json", 'w') as f:
                json.dump(timestamps, f, indent=2)
            logger.info(f"Created grouped folder: {folder_name}")
            return folder_name
    except Exception as e:
        logger.error(f"Error creating grouped folder: {e}")
    
    if folder_path.exists():
        shutil.rmtree(folder_path)
    return None

def extract_media_id(filename: str) -> Optional[str]:
    """Extract media ID from filename."""
    if 'thumbnail' in filename.lower():
        return None
    
    if 'b~' in filename:
        match = re.search(r'b~([^.]+)', filename, re.I)
        if match:
            return f'b~{match.group(1)}'
    
    match = re.search(r'media~zip-([A-F0-9\-]+)', filename, re.I)
    if match:
        return f'media~zip-{match.group(1)}'
    
    match = re.search(r'(media|overlay)~([A-F0-9\-]+)', filename, re.I)
    if match:
        return f'{match.group(1)}~{match.group(2)}'
    
    return None

def index_media_files(media_dir: Path) -> Dict[str, str]:
    """Create index of media ID to filename."""
    logger.info(f"Indexing media files in {media_dir}")
    media_index = {}
    
    if not media_dir.exists():
        return {}
    
    for item in media_dir.iterdir():
        if item.is_file():
            media_id = extract_media_id(item.name)
            if media_id:
                media_index[media_id] = item.name
        elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
            # Index files in folder
            for file_path in item.iterdir():
                if file_path.suffix.lower() == '.mp4':
                    media_id = extract_media_id(file_path.name)
                    if media_id:
                        media_index[media_id] = item.name  # Map to folder
    
    logger.info(f"Indexed {len(media_index)} media items")
    return media_index

def extract_mp4_timestamp(mp4_path: Path) -> Optional[int]:
    """Extract creation timestamp from MP4 file."""
    try:
        with open(mp4_path, "rb") as f:
            while True:
                header = f.read(8)
                if not header:
                    return None
                
                size = struct.unpack('>I', header[0:4])[0]
                atom_type = header[4:8]
                
                if atom_type == b'moov':
                    mvhd = f.read(8)
                    if mvhd[4:8] == b'mvhd':
                        version = f.read(1)[0]
                        f.seek(3, 1)
                        
                        if version == 0:
                            creation_time = struct.unpack('>I', f.read(4))[0]
                        else:
                            creation_time = struct.unpack('>Q', f.read(8))[0]
                        
                        return (creation_time - QUICKTIME_EPOCH_ADJUSTER) * 1000
                    return None
                
                if size == 1:
                    f.seek(struct.unpack('>Q', f.read(8))[0] - 16, 1)
                else:
                    f.seek(size - 8, 1)
    except Exception:
        return None

def map_media_to_messages(conversations: Dict[str, List], media_index: Dict[str, str], 
                         media_dir: Path) -> Tuple[Dict, Set[str]]:
    """Map media files to conversation messages."""
    logger.info("Mapping media to messages")
    mappings = defaultdict(dict)
    mapped_files = set()
    
    # Phase 1: Map by Media ID
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            media_ids_str = msg.get("Media IDs", "")
            if not media_ids_str:
                continue
            
            for media_id in media_ids_str.split('|'):
                media_id = media_id.strip()
                if media_id in media_index:
                    filename = media_index[media_id]
                    is_grouped = filename.endswith("_multipart") or filename.endswith("_grouped")
                    
                    if i not in mappings[conv_id]:
                        mappings[conv_id][i] = []
                    
                    mappings[conv_id][i].append({
                        "filename": filename,
                        "mapping_method": "media_id",
                        "is_grouped": is_grouped
                    })
                    mapped_files.add(filename)
    
    logger.info(f"Mapped {len(mapped_files)} files using Media IDs")
    
    # Phase 2: Map by timestamp
    unmapped_mp4s = []
    unmapped_folders = []
    
    for item in media_dir.iterdir():
        if item.name not in mapped_files:
            if item.is_file() and item.suffix.lower() == '.mp4':
                unmapped_mp4s.append(item)
            elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
                unmapped_folders.append(item)
    
    # Build message timestamp index
    msg_timestamps = []
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            ts = int(msg.get("Created(microseconds)", 0))
            if ts > 0:
                msg_timestamps.append((conv_id, i, ts))
    msg_timestamps.sort(key=lambda x: x[2])
    
    # Map MP4s by timestamp
    for mp4_file in unmapped_mp4s:
        timestamp = extract_mp4_timestamp(mp4_file)
        if timestamp:
            best_match = None
            min_diff = float('inf')
            
            for conv_id, msg_idx, msg_ts in msg_timestamps:
                diff = abs(timestamp - msg_ts)
                if diff < min_diff:
                    min_diff = diff
                    best_match = (conv_id, msg_idx)
            
            if best_match and min_diff <= TIMESTAMP_THRESHOLD_SECONDS * 1000:
                conv_id, msg_idx = best_match
                if msg_idx not in mappings[conv_id]:
                    mappings[conv_id][msg_idx] = []
                
                mappings[conv_id][msg_idx].append({
                    "filename": mp4_file.name,
                    "mapping_method": "timestamp",
                    "time_diff_seconds": round(min_diff / 1000.0, 1),
                    "is_grouped": False
                })
                mapped_files.add(mp4_file.name)
    
    # Map folders by timestamp
    for folder in unmapped_folders:
        timestamps_file = folder / "timestamps.json"
        min_timestamp = None
        
        if timestamps_file.exists():
            with open(timestamps_file) as f:
                data = json.load(f)
                for iso_ts in data.values():
                    from datetime import datetime
                    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1000)
                    if min_timestamp is None or ts < min_timestamp:
                        min_timestamp = ts
        
        if min_timestamp:
            best_match = None
            min_diff = float('inf')
            
            for conv_id, msg_idx, msg_ts in msg_timestamps:
                diff = abs(min_timestamp - msg_ts)
                if diff < min_diff:
                    min_diff = diff
                    best_match = (conv_id, msg_idx)
            
            if best_match and min_diff <= TIMESTAMP_THRESHOLD_SECONDS * 1000:
                conv_id, msg_idx = best_match
                if msg_idx not in mappings[conv_id]:
                    mappings[conv_id][msg_idx] = []
                
                mappings[conv_id][msg_idx].append({
                    "filename": folder.name,
                    "mapping_method": "timestamp",
                    "time_diff_seconds": round(min_diff / 1000.0, 1),
                    "is_grouped": True
                })
                mapped_files.add(folder.name)
    
    logger.info(f"Total files mapped: {len(mapped_files)}")
    return dict(mappings), mapped_files