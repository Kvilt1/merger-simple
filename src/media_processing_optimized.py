"""Optimized media processing with parallel FFmpeg operations."""

import hashlib
import json
import logging
import os
import re
import shutil
import struct
import subprocess
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from bisect import bisect_left
from tqdm import tqdm

from config import (
    TIMESTAMP_THRESHOLD_SECONDS,
    QUICKTIME_EPOCH_ADJUSTER,
    ensure_directory,
    format_timestamp
)
from optimization_utils import get_optimal_worker_count, get_memory_limit

logger = logging.getLogger(__name__)

def run_ffmpeg_merge_safe(media_file: Path, overlay_file: Path, output_file: Path) -> bool:
    """Thread-safe FFmpeg merge with optimized settings."""
    if not shutil.which("ffmpeg"):
        return False
    
    if output_file.exists():
        # Skip if already processed
        return True
    
    try:
        # Optimized FFmpeg command with hardware acceleration attempt
        command = [
            "ffmpeg", "-y",
            "-hwaccel", "auto",  # Try hardware acceleration
            "-i", str(media_file),
            "-i", str(overlay_file),
            "-filter_complex",
            "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
            "-map", "[vout]",
            "-map", "0:a?",
            "-map_metadata", "0",
            "-movflags", "+faststart",
            "-c:v", "libx264",
            "-preset", "veryfast",  # Already using veryfast
            "-crf", "18",
            "-c:a", "copy",
            "-threads", "0",  # Let FFmpeg decide thread count
            str(output_file)
        ]
        
        result = subprocess.run(
            command, 
            capture_output=True, 
            text=True, 
            timeout=300,
            check=False
        )
        
        if result.returncode == 0:
            # Preserve timestamps
            st = media_file.stat()
            os.utime(output_file, (st.st_atime, st.st_mtime))
            return True
            
    except subprocess.TimeoutExpired:
        logger.error(f"FFmpeg timeout for {media_file.name}")
        if output_file.exists():
            output_file.unlink()
    except Exception as e:
        logger.error(f"FFmpeg error for {media_file.name}: {e}")
        if output_file.exists():
            output_file.unlink()
    
    return False

def process_overlay_batch(args: Tuple[Path, Path, Path]) -> Tuple[str, bool]:
    """Process a single overlay merge (for multiprocessing)."""
    media_file, overlay_file, output_file = args
    success = run_ffmpeg_merge_safe(media_file, overlay_file, output_file)
    return media_file.name, success

def process_multipart_parallel(args: Tuple[str, List[Path], Path, Path]) -> Optional[str]:
    """Process multi-part video with identical overlay (for multiprocessing)."""
    date_str, media_files, overlay_file, temp_dir = args
    media_files_sorted = sorted(media_files, key=lambda x: x.name)
    folder_name = media_files_sorted[-1].stem + "_multipart"
    folder_path = temp_dir / folder_name
    
    try:
        ensure_directory(folder_path)
        timestamps = {}
        successful = 0
        
        for media_file in media_files_sorted:
            output_path = folder_path / media_file.name
            if run_ffmpeg_merge_safe(media_file, overlay_file, output_path):
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

def process_grouped_parallel(args: Tuple[str, List[Path], List[Path], Path]) -> Set[str]:
    """Process groups of overlays and media files (for multiprocessing)."""
    date_str, media_files, overlay_files, temp_dir = args
    merged = set()
    
    # Group overlays by hash
    overlay_groups = defaultdict(list)
    for overlay in overlay_files:
        file_hash = calculate_file_hash_cached(overlay)
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
                    if run_ffmpeg_merge_safe(media_group[0], overlay_group[0], output):
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
            if run_ffmpeg_merge_safe(media, overlay, output_path):
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

def merge_overlay_pairs_parallel(source_dir: Path, temp_dir: Path) -> Set[str]:
    """Parallel version of overlay merging with progress tracking."""
    logger.info("Starting parallel overlay merging process")
    
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
    
    # Prepare merge tasks
    merge_tasks = []
    merged_files = set()
    multipart_tasks = []
    grouped_tasks = []
    
    for date_str, files in files_by_date.items():
        media_files = files["media"]
        overlay_files = files["overlay"]
        
        # Simple pair
        if len(media_files) == 1 and len(overlay_files) == 1:
            output_file = temp_dir / media_files[0].name
            merge_tasks.append((media_files[0], overlay_files[0], output_file))
            merged_files.add(media_files[0].name)
            merged_files.add(overlay_files[0].name)
        
        # Multi-part with identical overlays
        elif len(overlay_files) > 1 and len(media_files) == len(overlay_files):
            # Check if overlays identical
            hashes = [calculate_file_hash_cached(f) for f in overlay_files]
            if len(set(h for h in hashes if h)) == 1:
                multipart_tasks.append((date_str, media_files, overlay_files[0], temp_dir))
                for f in media_files + overlay_files:
                    merged_files.add(f.name)
            else:
                # Process groups with different overlays
                grouped_tasks.append((date_str, media_files, overlay_files, temp_dir))
        
        # Mismatched counts - try grouping
        elif len(overlay_files) > 1 and len(media_files) > 1:
            grouped_tasks.append((date_str, media_files, overlay_files, temp_dir))
    
    # Process merges in parallel with progress bar
    if merge_tasks:
        logger.info(f"Processing {len(merge_tasks)} overlay pairs in parallel")
        
        workers = get_optimal_worker_count("ffmpeg")
        successful = 0
        
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_overlay_batch, task): task 
                      for task in merge_tasks}
            
            with tqdm(total=len(merge_tasks), desc="Merging overlays") as pbar:
                for future in as_completed(futures):
                    try:
                        filename, success = future.result(timeout=600)
                        if success:
                            successful += 1
                        pbar.update(1)
                        pbar.set_postfix({"Success": successful})
                    except Exception as e:
                        logger.error(f"Merge task failed: {e}")
                        pbar.update(1)
        
        logger.info(f"Successfully merged {successful}/{len(merge_tasks)} overlay pairs")
    
    # Process multipart tasks
    if multipart_tasks:
        logger.info(f"Processing {len(multipart_tasks)} multipart groups")
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_multipart_parallel, task): task 
                      for task in multipart_tasks}
            
            with tqdm(total=len(multipart_tasks), desc="Creating multipart folders") as pbar:
                for future in as_completed(futures):
                    try:
                        folder_name = future.result(timeout=600)
                        if folder_name:
                            logger.info(f"Created multipart: {folder_name}")
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Multipart task failed: {e}")
                        pbar.update(1)
    
    # Process grouped tasks
    if grouped_tasks:
        logger.info(f"Processing {len(grouped_tasks)} grouped overlay sets")
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_grouped_parallel, task): task 
                      for task in grouped_tasks}
            
            with tqdm(total=len(grouped_tasks), desc="Processing grouped overlays") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=600)
                        merged_files.update(result)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Grouped task failed: {e}")
                        pbar.update(1)
    
    logger.info(f"Overlay processing complete. Total merged: {len(merged_files)} files")
    return merged_files

def calculate_file_hash_cached(file_path: Path) -> Optional[str]:
    """Calculate MD5 hash with caching."""
    cache_key = f"{file_path.name}_{file_path.stat().st_mtime}"
    
    # Simple in-memory cache (could be improved with persistent cache)
    if not hasattr(calculate_file_hash_cached, '_cache'):
        calculate_file_hash_cached._cache = {}
    
    if cache_key in calculate_file_hash_cached._cache:
        return calculate_file_hash_cached._cache[cache_key]
    
    try:
        with open(file_path, 'rb') as f:
            hash_val = hashlib.md5(f.read()).hexdigest()
            calculate_file_hash_cached._cache[cache_key] = hash_val
            return hash_val
    except Exception:
        return None

def index_media_files_optimized(media_dir: Path) -> Dict[str, str]:
    """Optimized media indexing with hash table for O(1) lookup."""
    logger.info(f"Indexing media files in {media_dir}")
    media_index = {}
    
    if not media_dir.exists():
        return {}
    
    # Use os.scandir for faster directory traversal
    with os.scandir(media_dir) as entries:
        for entry in entries:
            if entry.is_file():
                media_id = extract_media_id(entry.name)
                if media_id:
                    media_index[media_id] = entry.name
            elif entry.is_dir() and (entry.name.endswith("_multipart") or 
                                    entry.name.endswith("_grouped")):
                # Index files in folder
                folder_path = media_dir / entry.name
                with os.scandir(folder_path) as folder_entries:
                    for file_entry in folder_entries:
                        if file_entry.is_file() and file_entry.name.endswith('.mp4'):
                            media_id = extract_media_id(file_entry.name)
                            if media_id:
                                media_index[media_id] = entry.name
    
    logger.info(f"Indexed {len(media_index)} media items")
    return media_index

def extract_media_id(filename: str) -> Optional[str]:
    """Extract media ID from filename (unchanged from original)."""
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

def get_media_type_from_extension(filename: str) -> Optional[str]:
    """Determine Snapchat media type from file extension."""
    ext = Path(filename).suffix.lower()
    
    # Image types
    if ext in ['.jpeg', '.jpg', '.png', '.webp']:
        return "IMAGE"
    
    # Video types  
    if ext in ['.mp4', '.mov']:
        return "VIDEO"
    
    return None

def extract_mp4_timestamp(mp4_path: Path) -> Optional[int]:
    """Extract creation timestamp from MP4 file (unchanged)."""
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

def map_media_to_messages_optimized(conversations: Dict[str, List], 
                                   media_index: Dict[str, str], 
                                   media_dir: Path) -> Tuple[Dict, Set[str]]:
    """Optimized media mapping with binary search for timestamps."""
    logger.info("Mapping media to messages with optimized algorithms")
    mappings = defaultdict(dict)
    mapped_files = set()
    
    # Phase 1: Map by Media ID (O(1) lookup with hash table)
    with tqdm(desc="Mapping by Media ID") as pbar:
        for conv_id, messages in conversations.items():
            for i, msg in enumerate(messages):
                media_ids_str = msg.get("Media IDs", "")
                if not media_ids_str:
                    continue
                
                for media_id in media_ids_str.split('|'):
                    media_id = media_id.strip()
                    if media_id in media_index:  # O(1) lookup
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
                pbar.update(1)
    
    logger.info(f"Mapped {len(mapped_files)} files using Media IDs")
    
    # Phase 2: Build sorted timestamp index for binary search
    msg_timestamps_sorted = []
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            ts = int(msg.get("Created(microseconds)", 0))
            if ts > 0:
                msg_timestamps_sorted.append((ts, conv_id, i))
    
    msg_timestamps_sorted.sort(key=lambda x: x[0])  # Sort by timestamp
    timestamps_only = [ts for ts, _, _ in msg_timestamps_sorted]
    
    # Phase 3: Map unmapped media by timestamp using binary search
    unmapped_mp4s = []
    
    for item in media_dir.iterdir():
        if item.name not in mapped_files:
            if item.is_file() and item.suffix.lower() == '.mp4':
                unmapped_mp4s.append(item)
    
    if unmapped_mp4s and msg_timestamps_sorted:
        with tqdm(total=len(unmapped_mp4s), desc="Mapping by timestamp") as pbar:
            for mp4_file in unmapped_mp4s:
                timestamp = extract_mp4_timestamp(mp4_file)
                if timestamp:
                    # Binary search for closest timestamp
                    idx = bisect_left(timestamps_only, timestamp)
                    
                    best_match = None
                    min_diff = float('inf')
                    
                    # Check nearby timestamps
                    for check_idx in range(max(0, idx-1), min(len(msg_timestamps_sorted), idx+2)):
                        ts, conv_id, msg_idx = msg_timestamps_sorted[check_idx]
                        diff = abs(timestamp - ts)
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
                
                pbar.update(1)
    
    # Phase 3: Optimized date correlation mapping
    logger.info("Phase 3: Date-based correlation for remaining orphans")
    
    # Build date index for snaps (O(n) preprocessing)
    snaps_by_date_type = defaultdict(list)
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            if msg.get("Type") == "snap":
                created = msg.get("Created", "")
                match = re.match(r'(\d{4}-\d{2}-\d{2})', created)
                if match:
                    date_str = match.group(1)
                    media_type = msg.get("Media Type")
                    if media_type:
                        snaps_by_date_type[(date_str, media_type)].append((conv_id, i, msg))
    
    # Group unmapped b~ files by date
    unmapped_by_date = defaultdict(list)
    for item in media_dir.iterdir():
        if item.name not in mapped_files and item.is_file():
            if 'b~' in item.name:
                match = re.match(r'(\d{4}-\d{2}-\d{2})', item.name)
                if match:
                    date_str = match.group(1)
                    unmapped_by_date[date_str].append(item)
    
    # Process dates with exactly ONE b~ file
    date_correlations = 0
    with tqdm(total=len(unmapped_by_date), desc="Date correlation mapping") as pbar:
        for date_str, files in unmapped_by_date.items():
            if len(files) == 1:
                media_file = files[0]
                media_type = get_media_type_from_extension(media_file.name)
                
                if media_type:
                    # O(1) lookup for matching snaps
                    matching_snaps = snaps_by_date_type.get((date_str, media_type), [])
                    
                    # If exactly ONE snap matches, create mapping
                    if len(matching_snaps) == 1:
                        conv_id, msg_idx, snap = matching_snaps[0]
                        
                        if msg_idx not in mappings[conv_id]:
                            mappings[conv_id][msg_idx] = []
                        
                        mappings[conv_id][msg_idx].append({
                            "filename": media_file.name,
                            "mapping_method": "date_correlation",
                            "confidence": "high",
                            "is_grouped": False
                        })
                        mapped_files.add(media_file.name)
                        date_correlations += 1
                        
                        logger.debug(f"Date correlation: {media_file.name} → {snap.get('From', 'unknown')} ({date_str})")
            pbar.update(1)
    
    if date_correlations > 0:
        logger.info(f"Successfully mapped {date_correlations} files using date correlation")
    
    logger.info(f"Total files mapped: {len(mapped_files)}")
    return dict(mappings), mapped_files