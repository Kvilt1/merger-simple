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
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

from config import (
    TIMESTAMP_THRESHOLD_SECONDS,
    QUICKTIME_EPOCH_ADJUSTER,
    ensure_directory,
    format_timestamp
)

logger = logging.getLogger(__name__)


def _ffmpeg_worker(args: Tuple[Path, Path, Path]) -> Optional[Tuple[str, Optional[int]]]:
    """
    A picklable top-level worker function for FFmpeg merging.
    On success, returns the original media filename and its extracted timestamp.
    """
    media_file, overlay_file, output_path = args
    if run_ffmpeg_merge(media_file, overlay_file, output_path):
        timestamp = extract_mp4_timestamp(media_file)
        return (media_file.name, timestamp)
    return None


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

def merge_overlay_pairs(source_dir: Path, temp_dir: Path) -> Tuple[Set[str], Dict[str, Any]]:
    """Find and merge media/overlay pairs. Returns merged files and statistics."""
    logger.info("=" * 60)
    logger.info("Starting OVERLAY MERGING phase")
    logger.info("=" * 60)
    
    stats = {
        'total_media': 0,
        'total_overlay': 0,
        'simple_pairs_attempted': 0,
        'simple_pairs_succeeded': 0,
        'multipart_attempted': 0,
        'multipart_succeeded': 0,
        'grouped_attempted': 0,
        'grouped_succeeded': 0,
        'ffmpeg_errors': [],
        'total_merged': 0
    }
    
    if not shutil.which("ffmpeg"):
        logger.warning("FFmpeg not found. Skipping overlay merging.")
        return set(), stats
    
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
            stats['total_media'] += 1
        elif "_overlay~" in file_path.name:
            files_by_date[date_str]["overlay"].append(file_path)
            stats['total_overlay'] += 1
    
    logger.info(f"Found {stats['total_media']} media files and {stats['total_overlay']} overlay files")
    
    merged_files = set()
    
    for date_str, files in files_by_date.items():
        media_files = files["media"]
        overlay_files = files["overlay"]
        
        # Simple pair
        if len(media_files) == 1 and len(overlay_files) == 1:
            stats['simple_pairs_attempted'] += 1
            output_file = temp_dir / media_files[0].name
            if run_ffmpeg_merge(media_files[0], overlay_files[0], output_file):
                merged_files.add(media_files[0].name)
                merged_files.add(overlay_files[0].name)
                stats['simple_pairs_succeeded'] += 1
                stats['total_merged'] += 1
            else:
                stats['ffmpeg_errors'].append(media_files[0].name)
        
        # Multi-part with identical overlays
        elif len(overlay_files) > 1 and len(media_files) == len(overlay_files):
            # Check if overlays identical
            hashes = [calculate_file_hash(f) for f in overlay_files]
            if len(set(h for h in hashes if h)) == 1:
                stats['multipart_attempted'] += 1
                folder_name, folder_stats = process_multipart(date_str, media_files, overlay_files[0], temp_dir)
                if folder_name:
                    for f in media_files + overlay_files:
                        merged_files.add(f.name)
                    stats['multipart_succeeded'] += 1
                    stats['total_merged'] += folder_stats['successful']
            else:
                # Process groups with different overlays
                stats['grouped_attempted'] += 1
                result, group_stats = process_grouped_overlays(date_str, media_files, overlay_files, temp_dir)
                merged_files.update(result)
                if group_stats['successful'] > 0:
                    stats['grouped_succeeded'] += 1
                    stats['total_merged'] += group_stats['successful']
        
        # Mismatched counts - try grouping
        elif len(overlay_files) > 1 and len(media_files) > 1:
            stats['grouped_attempted'] += 1
            result, group_stats = process_grouped_overlays(date_str, media_files, overlay_files, temp_dir)
            merged_files.update(result)
            if group_stats['successful'] > 0:
                stats['grouped_succeeded'] += 1
                stats['total_merged'] += group_stats['successful']
    
    # Log statistics
    logger.info("=" * 60)
    logger.info("OVERLAY MERGING RESULTS:")
    
    if stats['simple_pairs_attempted'] > 0:
        pct = (stats['simple_pairs_succeeded'] / stats['simple_pairs_attempted']) * 100
        logger.info(f"  Simple pairs: [{stats['simple_pairs_succeeded']}]/[{stats['simple_pairs_attempted']}] ({pct:.1f}%)")
    
    if stats['multipart_attempted'] > 0:
        pct = (stats['multipart_succeeded'] / stats['multipart_attempted']) * 100
        logger.info(f"  Multipart folders: [{stats['multipart_succeeded']}]/[{stats['multipart_attempted']}] ({pct:.1f}%)")
    
    if stats['grouped_attempted'] > 0:
        pct = (stats['grouped_succeeded'] / stats['grouped_attempted']) * 100
        logger.info(f"  Grouped folders: [{stats['grouped_succeeded']}]/[{stats['grouped_attempted']}] ({pct:.1f}%)")
    
    logger.info(f"  Total merged operations: {stats['total_merged']}")
    
    if stats['ffmpeg_errors']:
        logger.warning(f"  FFmpeg errors on {len(stats['ffmpeg_errors'])} files")
    
    logger.info("=" * 60)
    
    return merged_files, stats

def process_multipart(date_str: str, media_files: List[Path], overlay_file: Path, temp_dir: Path) -> Tuple[Optional[str], Dict[str, int]]:
    """Process multi-part video with identical overlay using a process pool. Returns folder name and statistics."""
    media_files_sorted = sorted(media_files, key=lambda x: x.name)
    folder_name = media_files_sorted[-1].stem + "_multipart"
    folder_path = temp_dir / folder_name
    
    stats = {'attempted': len(media_files), 'successful': 0, 'failed': 0}
    
    try:
        ensure_directory(folder_path)
        
        tasks = [
            (media_file, overlay_file, folder_path / media_file.name)
            for media_file in media_files_sorted
        ]
        
        logger.info(f"Processing multipart group: {folder_name} with {len(tasks)} files")
        
        timestamps = {}
        
        num_processes = max(1, cpu_count() - 1)
        with Pool(processes=num_processes) as pool:
            results = pool.map(_ffmpeg_worker, tasks)

        for result in results:
            if result:
                stats['successful'] += 1
                filename, timestamp = result
                if timestamp:
                    timestamps[filename] = format_timestamp(timestamp)
            else:
                stats['failed'] += 1
        
        success_pct = (stats['successful'] / stats['attempted']) * 100 if stats['attempted'] > 0 else 0
        logger.info(f"  Multipart merging: [{stats['successful']}]/[{stats['attempted']}] ({success_pct:.1f}%) - {folder_name}")
        
        if stats['successful'] > 0:
            with open(folder_path / "timestamps.json", 'w', encoding='utf-8') as f:
                json.dump(timestamps, f, indent=2)
            return folder_name, stats
        else:
            logger.warning(f"  All merges failed for multipart {folder_name}")
            if folder_path.exists():
                shutil.rmtree(folder_path)

    except Exception as e:
        logger.error(f"Error processing multi-part for {date_str}: {e}")
        if folder_path.exists():
            shutil.rmtree(folder_path)
    
    return None, stats

def process_grouped_overlays(date_str: str, media_files: List[Path], overlay_files: List[Path], temp_dir: Path) -> Tuple[Set[str], Dict[str, int]]:
    """Process groups of overlays and media files. Returns merged files and statistics."""
    merged = set()
    stats = {'attempted': 0, 'successful': 0, 'failed': 0}
    
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
    
    logger.info(f"Processing grouped overlays: {len(media_groups)} media groups, {len(overlay_groups)} overlay groups")
    
    # Match groups by count
    overlay_list = list(overlay_groups.values())
    
    for media_group in media_groups:
        for i, overlay_group in enumerate(overlay_list):
            if len(media_group) == len(overlay_group):
                stats['attempted'] += len(media_group)
                
                if len(media_group) == 1:
                    # Single pair
                    output = temp_dir / media_group[0].name
                    if run_ffmpeg_merge(media_group[0], overlay_group[0], output):
                        merged.add(media_group[0].name)
                        merged.add(overlay_group[0].name)
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                else:
                    # Multi-file group
                    folder_name, folder_stats = create_grouped_folder(media_group, overlay_group, temp_dir)
                    if folder_name:
                        for f in media_group + overlay_group:
                            merged.add(f.name)
                        stats['successful'] += folder_stats['successful']
                        stats['failed'] += folder_stats['failed']
                
                overlay_list.pop(i)
                break
    
    if stats['attempted'] > 0:
        success_pct = (stats['successful'] / stats['attempted']) * 100
        logger.info(f"  Grouped processing: [{stats['successful']}]/[{stats['attempted']}] ({success_pct:.1f}%)")
    
    return merged, stats

def create_grouped_folder(media_files: List[Path], overlay_files: List[Path], temp_dir: Path) -> Tuple[Optional[str], Dict[str, int]]:
    """Create folder for grouped media/overlay pairs using a process pool. Returns folder name and statistics."""
    media_sorted = sorted(media_files, key=lambda x: x.name)
    overlay_sorted = sorted(overlay_files, key=lambda x: x.name)
    
    folder_name = media_sorted[-1].stem + "_grouped"
    folder_path = temp_dir / folder_name
    
    stats = {'attempted': len(media_files), 'successful': 0, 'failed': 0}
    
    try:
        ensure_directory(folder_path)

        tasks = [
            (media, overlay, folder_path / media.name)
            for media, overlay in zip(media_sorted, overlay_sorted)
        ]

        logger.info(f"Processing grouped folder: {folder_name} with {len(tasks)} files")

        timestamps = {}
        
        num_processes = max(1, cpu_count() - 1)
        with Pool(processes=num_processes) as pool:
            results = pool.map(_ffmpeg_worker, tasks)

        for result in results:
            if result:
                stats['successful'] += 1
                filename, timestamp = result
                if timestamp:
                    timestamps[filename] = format_timestamp(timestamp)
            else:
                stats['failed'] += 1
        
        success_pct = (stats['successful'] / stats['attempted']) * 100 if stats['attempted'] > 0 else 0
        logger.info(f"  Grouped merging: [{stats['successful']}]/[{stats['attempted']}] ({success_pct:.1f}%) - {folder_name}")
        
        if stats['successful'] > 0:
            with open(folder_path / "timestamps.json", 'w', encoding='utf-8') as f:
                json.dump(timestamps, f, indent=2)
            return folder_name, stats
        else:
            logger.warning(f"  All merges failed for grouped {folder_name}")
            if folder_path.exists():
                shutil.rmtree(folder_path)

    except Exception as e:
        logger.error(f"Error creating grouped folder: {e}")
        if folder_path.exists():
            shutil.rmtree(folder_path)
            
    return None, stats

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

def index_media_files(media_dir: Path) -> Tuple[Dict[str, str], Dict[str, int]]:
    """Create index of media ID to filename. Returns index and statistics."""
    logger.info("=" * 60)
    logger.info("Starting MEDIA INDEXING phase")
    logger.info("=" * 60)
    
    media_index = {}
    stats = {
        'total_files': 0,
        'regular_files': 0,
        'multipart_folders': 0,
        'grouped_folders': 0,
        'extracted_ids': 0,
        'no_id_files': []
    }
    
    if not media_dir.exists():
        logger.warning(f"Media directory {media_dir} does not exist")
        return {}, stats
    
    for item in media_dir.iterdir():
        if item.is_file():
            stats['total_files'] += 1
            stats['regular_files'] += 1
            media_id = extract_media_id(item.name)
            if media_id:
                media_index[media_id] = item.name
                stats['extracted_ids'] += 1
            else:
                stats['no_id_files'].append(item.name)
                
        elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
            if item.name.endswith("_multipart"):
                stats['multipart_folders'] += 1
            else:
                stats['grouped_folders'] += 1
                
            # Index files in folder
            for file_path in item.iterdir():
                if file_path.suffix.lower() == '.mp4':
                    stats['total_files'] += 1
                    media_id = extract_media_id(file_path.name)
                    if media_id:
                        media_index[media_id] = item.name  # Map to folder
                        stats['extracted_ids'] += 1
                    else:
                        stats['no_id_files'].append(file_path.name)
    
    # Log statistics
    logger.info("MEDIA INDEXING RESULTS:")
    logger.info(f"  Total files processed: {stats['total_files']}")
    logger.info(f"    - Regular files: {stats['regular_files']}")
    logger.info(f"    - Files in multipart folders: {stats['multipart_folders']}")
    logger.info(f"    - Files in grouped folders: {stats['grouped_folders']}")
    
    if stats['total_files'] > 0:
        success_pct = (stats['extracted_ids'] / stats['total_files']) * 100
        logger.info(f"  Media ID extraction: [{stats['extracted_ids']}]/[{stats['total_files']}] ({success_pct:.1f}%)")
    
    if stats['no_id_files'] and len(stats['no_id_files']) <= 10:
        logger.debug(f"  Files without extractable IDs: {stats['no_id_files'][:10]}")
    elif stats['no_id_files']:
        logger.debug(f"  {len(stats['no_id_files'])} files without extractable IDs (showing first 10)")
    
    logger.info("=" * 60)
    
    return media_index, stats

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
                         media_dir: Path) -> Tuple[Dict, Set[str], Dict[str, Any]]:
    """Map media files to conversation messages. Returns mappings, mapped files, and statistics."""
    logger.info("=" * 60)
    logger.info("Starting MEDIA MAPPING phase")
    logger.info("=" * 60)
    
    mappings = defaultdict(dict)
    mapped_files = set()
    
    stats = {
        'total_messages_with_media_ids': 0,
        'media_ids_found': 0,
        'media_ids_not_found': 0,
        'mapped_by_id': 0,
        'mapped_by_timestamp': 0,
        'mp4s_with_timestamp': 0,
        'mp4s_without_timestamp': 0,
        'timestamp_matches': 0,
        'folders_mapped': 0,
        'unmapped_files': 0
    }
    
    # Phase 1: Map by Media ID
    logger.info("Phase 1: Mapping by Media ID...")
    
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            media_ids_str = msg.get("Media IDs", "")
            if not media_ids_str:
                continue
            
            stats['total_messages_with_media_ids'] += 1
            
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
                    stats['media_ids_found'] += 1
                    stats['mapped_by_id'] += 1
                else:
                    stats['media_ids_not_found'] += 1
    
    if stats['total_messages_with_media_ids'] > 0:
        found_pct = (stats['media_ids_found'] / (stats['media_ids_found'] + stats['media_ids_not_found'])) * 100 if (stats['media_ids_found'] + stats['media_ids_not_found']) > 0 else 0
        logger.info(f"  Media ID mapping: [{stats['media_ids_found']}]/[{stats['media_ids_found'] + stats['media_ids_not_found']}] ({found_pct:.1f}%)")
    
    # Phase 2: Map by timestamp
    logger.info("Phase 2: Mapping by timestamp...")
    
    unmapped_mp4s = []
    unmapped_folders = []
    
    for item in media_dir.iterdir():
        if item.name not in mapped_files:
            if item.is_file() and item.suffix.lower() == '.mp4':
                unmapped_mp4s.append(item)
            elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
                unmapped_folders.append(item)
    
    logger.info(f"  Found {len(unmapped_mp4s)} unmapped MP4 files and {len(unmapped_folders)} unmapped folders")
    
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
            stats['mp4s_with_timestamp'] += 1
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
                stats['mapped_by_timestamp'] += 1
                stats['timestamp_matches'] += 1
        else:
            stats['mp4s_without_timestamp'] += 1
    
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
                stats['folders_mapped'] += 1
                stats['mapped_by_timestamp'] += 1
    
    # Calculate unmapped
    all_media_count = len(unmapped_mp4s) + len(unmapped_folders)
    for item in media_dir.iterdir():
        if item.is_file() and item.name in mapped_files:
            all_media_count += 1
    
    stats['unmapped_files'] = all_media_count - len(mapped_files) if all_media_count > len(mapped_files) else 0
    
    # Log timestamp mapping statistics
    if len(unmapped_mp4s) > 0:
        ts_extract_pct = (stats['mp4s_with_timestamp'] / len(unmapped_mp4s)) * 100
        logger.info(f"  Timestamp extraction: [{stats['mp4s_with_timestamp']}]/[{len(unmapped_mp4s)}] MP4s ({ts_extract_pct:.1f}%)")
    
    if stats['mp4s_with_timestamp'] + len(unmapped_folders) > 0:
        ts_match_pct = (stats['timestamp_matches'] + stats['folders_mapped']) / (stats['mp4s_with_timestamp'] + len(unmapped_folders)) * 100
        logger.info(f"  Timestamp matching: [{stats['timestamp_matches'] + stats['folders_mapped']}]/[{stats['mp4s_with_timestamp'] + len(unmapped_folders)}] ({ts_match_pct:.1f}%)")
    
    logger.info("MEDIA MAPPING RESULTS:")
    logger.info(f"  Total mapped: {len(mapped_files)} files")
    logger.info(f"    - By Media ID: {stats['mapped_by_id']}")
    logger.info(f"    - By timestamp: {stats['mapped_by_timestamp']}")
    logger.info(f"  Unmapped: {stats['unmapped_files']} files")
    
    logger.info("=" * 60)
    
    return mappings, mapped_files, stats