# map_media.py
import logging
import re
import shutil
import struct
import subprocess
import os
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timezone
from collections import defaultdict

# Import helper functions from utils.py
from utils import load_json, save_json, ensure_directory

# --- Configuration ---
# You can adjust these settings
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
TIMESTAMP_THRESHOLD_SECONDS = 60  # How close an MP4's time must be to a message's time
QUICKTIME_EPOCH_ADJUSTER = 2082844800 # Seconds between QuickTime epoch (1904) and Unix epoch (1970)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Core Functions ---

def merge_overlay_pairs(source_dir: Path, temp_dir: Path) -> set:
    """
    Finds and merges media/overlay pairs using FFmpeg.
    Handles both single pairs and multi-part videos with duplicate overlays.
    """
    logging.info("--- Starting Overlay Merging Process ---")

    if not shutil.which("ffmpeg"):
        logging.warning("FFmpeg not found. Skipping overlay merging.")
        logging.warning("Please install FFmpeg to enable this feature.")
        return set()

    ensure_directory(temp_dir)

    # Group by date (ignore thumbnails and media~zip stubs)
    files_by_date = defaultdict(lambda: {"media": [], "overlay": []})
    for file_path in source_dir.iterdir():
        if not file_path.is_file():
            continue
        m = re.match(r"(\d{4}-\d{2}-\d{2})", file_path.name)
        if not m:
            continue
        date_str = m.group(1)
        name_lower = file_path.name.lower()
        if "_media~" in file_path.name and "thumbnail" not in name_lower and "media~zip-" not in file_path.name:
            files_by_date[date_str]["media"].append(file_path)
        elif "_overlay~" in file_path.name:
            files_by_date[date_str]["overlay"].append(file_path)

    merged_source_files = set()
    merged_count = 0

    for date_str, files in files_by_date.items():
        medias = files["media"]
        overlays = files["overlay"]

        # Check for multi-part video conditions
        if len(overlays) > 1 and len(medias) == len(overlays):
            # Check if all overlays are identical
            if are_overlays_identical(overlays):
                logging.info(f"Detected multi-part video for {date_str}: {len(medias)} parts with identical overlays")
                
                # Process multi-part video
                folder_name = process_multipart_video(
                    date_str, medias, overlays[0], temp_dir
                )
                
                if folder_name:
                    # Mark all source files as processed
                    for media_file in medias:
                        merged_source_files.add(media_file.name)
                    for overlay_file in overlays:
                        merged_source_files.add(overlay_file.name)
                    merged_count += 1
                    logging.info(f"Successfully processed multi-part video: {folder_name}")
            else:
                # NEW CASE 3: Multiple non-identical overlays
                logging.info(f"Detected multiple non-identical overlays for {date_str}: {len(overlays)} overlays, {len(medias)} media files")
                
                # Group overlays by hash
                overlay_groups = group_overlays_by_hash(overlays)
                logging.info(f"Found {len(overlay_groups)} unique overlay groups")
                
                # Group media files by timestamp
                media_groups = extract_and_group_media_by_timestamp(medias, TIMESTAMP_THRESHOLD_SECONDS)
                logging.info(f"Found {len(media_groups)} media timestamp groups")
                
                # Match groups based on count
                matched_groups = match_overlay_and_media_groups(overlay_groups, media_groups)
                logging.info(f"Successfully matched {len(matched_groups)} group pairs")
                
                # Process each matched group
                for overlay_group, media_group in matched_groups:
                    # Check if this is a single-file group
                    if len(media_group) == 1 and len(overlay_group) == 1:
                        # Handle as single file merge
                        media_file = media_group[0]
                        overlay_file = overlay_group[0]
                        output_file = temp_dir / media_file.name
                        
                        logging.info(f"Processing single grouped file: {media_file.name} + {overlay_file.name}")
                        
                        try:
                            command = [
                                "ffmpeg",
                                "-y",
                                "-i", str(media_file),     # [0] base video
                                "-i", str(overlay_file),   # [1] overlay (alpha)
                                "-filter_complex",
                                "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
                                "-map", "[vout]",          # map filtered video
                                "-map", "0:a?",            # optional audio from base
                                "-map_metadata", "0",      # copy all tags from base
                                "-movflags", "+faststart",
                                "-c:v", "libx264",
                                "-preset", "veryfast",
                                "-crf", "18",
                                "-c:a", "copy",
                                str(output_file),
                            ]
                            subprocess.run(command, check=True, capture_output=True, text=True)
                            
                            # Mark originals as processed
                            merged_source_files.add(media_file.name)
                            merged_source_files.add(overlay_file.name)
                            
                            # Preserve file times
                            try:
                                st = media_file.stat()
                                os.utime(output_file, (st.st_atime, st.st_mtime))
                            except Exception as e:
                                logging.debug(f"Could not copy file times for {output_file.name}: {e}")
                            
                            merged_count += 1
                            
                        except subprocess.CalledProcessError as e:
                            logging.error(f"FFmpeg failed for {media_file.name}: {e.stderr}")
                        except Exception as e:
                            logging.error(f"An error occurred during single grouped merge: {e}")
                    else:
                        # Handle as multi-file group (create folder)
                        folder_name = process_grouped_multipart_video(
                            date_str, media_group, overlay_group, temp_dir
                        )
                        
                        if folder_name:
                            # Mark files as processed
                            for media_file in media_group:
                                merged_source_files.add(media_file.name)
                            for overlay_file in overlay_group:
                                merged_source_files.add(overlay_file.name)
                            merged_count += 1
                            logging.info(f"Successfully processed grouped multi-part: {folder_name}")
        
        # Check for multiple files with mismatched counts (also try Case 3 logic)
        elif len(overlays) > 1 and len(medias) > 1:
            # Try the grouping approach even with mismatched counts
            logging.info(f"Detected multiple files with mismatched counts for {date_str}: {len(overlays)} overlays, {len(medias)} media files")
            
            # Group overlays by hash
            overlay_groups = group_overlays_by_hash(overlays)
            logging.info(f"Found {len(overlay_groups)} unique overlay groups")
            
            # Group media files by timestamp
            media_groups = extract_and_group_media_by_timestamp(medias, TIMESTAMP_THRESHOLD_SECONDS)
            logging.info(f"Found {len(media_groups)} media timestamp groups")
            
            # Match groups based on count
            matched_groups = match_overlay_and_media_groups(overlay_groups, media_groups)
            
            if matched_groups:
                logging.info(f"Successfully matched {len(matched_groups)} group pairs despite mismatched counts")
                
                # Process each matched group
                for overlay_group, media_group in matched_groups:
                    # Check if this is a single-file group
                    if len(media_group) == 1 and len(overlay_group) == 1:
                        # Handle as single file merge
                        media_file = media_group[0]
                        overlay_file = overlay_group[0]
                        output_file = temp_dir / media_file.name
                        
                        logging.info(f"Processing single grouped file: {media_file.name} + {overlay_file.name}")
                        
                        try:
                            command = [
                                "ffmpeg",
                                "-y",
                                "-i", str(media_file),     # [0] base video
                                "-i", str(overlay_file),   # [1] overlay (alpha)
                                "-filter_complex",
                                "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
                                "-map", "[vout]",          # map filtered video
                                "-map", "0:a?",            # optional audio from base
                                "-map_metadata", "0",      # copy all tags from base
                                "-movflags", "+faststart",
                                "-c:v", "libx264",
                                "-preset", "veryfast",
                                "-crf", "18",
                                "-c:a", "copy",
                                str(output_file),
                            ]
                            subprocess.run(command, check=True, capture_output=True, text=True)
                            
                            # Mark originals as processed
                            merged_source_files.add(media_file.name)
                            merged_source_files.add(overlay_file.name)
                            
                            # Preserve file times
                            try:
                                st = media_file.stat()
                                os.utime(output_file, (st.st_atime, st.st_mtime))
                            except Exception as e:
                                logging.debug(f"Could not copy file times for {output_file.name}: {e}")
                            
                            merged_count += 1
                            
                        except subprocess.CalledProcessError as e:
                            logging.error(f"FFmpeg failed for {media_file.name}: {e.stderr}")
                        except Exception as e:
                            logging.error(f"An error occurred during single grouped merge: {e}")
                    else:
                        # Handle as multi-file group (create folder)
                        folder_name = process_grouped_multipart_video(
                            date_str, media_group, overlay_group, temp_dir
                        )
                        
                        if folder_name:
                            # Mark files as processed
                            for media_file in media_group:
                                merged_source_files.add(media_file.name)
                            for overlay_file in overlay_group:
                                merged_source_files.add(overlay_file.name)
                            merged_count += 1
                            logging.info(f"Successfully processed grouped multi-part: {folder_name}")
            else:
                logging.warning(f"Could not match any groups for {date_str}")
        
        # Handle single media/overlay pairs
        elif len(medias) == 1 and len(overlays) == 1:
            media_file = medias[0]
            overlay_file = overlays[0]
            output_file = temp_dir / media_file.name

            logging.info(f"Merging pair for {date_str}: {media_file.name} + {overlay_file.name}")

            try:
                command = [
                    "ffmpeg",
                    "-y",
                    "-i", str(media_file),     # [0] base video
                    "-i", str(overlay_file),   # [1] overlay (alpha)
                    "-filter_complex",
                    "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
                    "-map", "[vout]",          # map filtered video
                    "-map", "0:a?",            # optional audio from base
                    "-map_metadata", "0",      # copy all tags from base
                    "-movflags", "+faststart",
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", "18",
                    "-c:a", "copy",
                    str(output_file),
                ]
                subprocess.run(command, check=True, capture_output=True, text=True)

                # Mark originals so we don't copy them into temp later
                merged_source_files.add(media_file.name)
                merged_source_files.add(overlay_file.name)

                # Make the merged file carry the original filesystem times
                try:
                    st = media_file.stat()
                    os.utime(output_file, (st.st_atime, st.st_mtime))
                except Exception as e:
                    logging.debug(f"Could not copy file times for {output_file.name}: {e}")

                merged_count += 1

            except subprocess.CalledProcessError as e:
                logging.error(f"FFmpeg failed for {media_file.name}: {e.stderr}")
            except Exception as e:
                logging.error(f"An error occurred during merging: {e}")

    logging.info(f"Successfully merged {merged_count} overlay pairs.")
    return merged_source_files

def are_overlays_identical(overlay_files: List[Path]) -> bool:
    """
    Check if all overlay files have identical content by comparing their hashes.
    """
    if len(overlay_files) < 2:
        return False
    
    hashes = []
    for overlay_file in overlay_files:
        try:
            with open(overlay_file, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
                hashes.append(file_hash)
        except Exception as e:
            logging.error(f"Error hashing overlay file {overlay_file}: {e}")
            return False
    
    # Check if all hashes are identical
    return len(set(hashes)) == 1

def group_overlays_by_hash(overlay_files: List[Path]) -> Dict[str, List[Path]]:
    """
    Group overlay files by their content hash.
    Returns a dictionary mapping hash to list of overlay files.
    """
    hash_groups = defaultdict(list)
    
    for overlay_file in overlay_files:
        try:
            with open(overlay_file, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
                hash_groups[file_hash].append(overlay_file)
        except Exception as e:
            logging.error(f"Error hashing overlay file {overlay_file}: {e}")
    
    return dict(hash_groups)


def extract_and_group_media_by_timestamp(media_files: List[Path], threshold_seconds: int) -> List[List[Path]]:
    """
    Group media files by their timestamps.
    Files within threshold_seconds of each other are grouped together.
    """
    # Extract timestamps for all media files
    media_with_timestamps = []
    for media_file in media_files:
        timestamp = extract_mp4_timestamp(media_file)
        if timestamp:
            media_with_timestamps.append((media_file, timestamp))
        else:
            logging.warning(f"Could not extract timestamp from {media_file.name}")
    
    # Sort by timestamp
    media_with_timestamps.sort(key=lambda x: x[1])
    
    # Group files with close timestamps
    groups = []
    if media_with_timestamps:
        current_group = [media_with_timestamps[0][0]]
        current_timestamp = media_with_timestamps[0][1]
        
        for media_file, timestamp in media_with_timestamps[1:]:
            # Check if within threshold (in milliseconds)
            if abs(timestamp - current_timestamp) <= threshold_seconds * 1000:
                current_group.append(media_file)
            else:
                groups.append(current_group)
                current_group = [media_file]
                current_timestamp = timestamp
        
        # Add the last group
        groups.append(current_group)
    
    return groups


def match_overlay_and_media_groups(
    overlay_groups: Dict[str, List[Path]], 
    media_groups: List[List[Path]]
) -> List[Tuple[List[Path], List[Path]]]:
    """
    Match overlay groups with media groups based on file count.
    Prioritizes unique count matches.
    Returns list of (overlay_group, media_group) tuples.
    """
    # Convert overlay groups dict to list of groups
    overlay_group_list = list(overlay_groups.values())
    
    # Create count-based dictionaries
    overlay_counts = defaultdict(list)
    for idx, group in enumerate(overlay_group_list):
        overlay_counts[len(group)].append((idx, group))
    
    media_counts = defaultdict(list)
    for idx, group in enumerate(media_groups):
        media_counts[len(group)].append((idx, group))
    
    matched_pairs = []
    used_overlay_indices = set()
    used_media_indices = set()
    
    # First pass: match unique counts
    for count in overlay_counts.keys():
        if count in media_counts:
            overlay_groups_with_count = overlay_counts[count]
            media_groups_with_count = media_counts[count]
            
            # If both have exactly one group with this count, it's a unique match
            if len(overlay_groups_with_count) == 1 and len(media_groups_with_count) == 1:
                overlay_idx, overlay_group = overlay_groups_with_count[0]
                media_idx, media_group = media_groups_with_count[0]
                
                if overlay_idx not in used_overlay_indices and media_idx not in used_media_indices:
                    matched_pairs.append((overlay_group, media_group))
                    used_overlay_indices.add(overlay_idx)
                    used_media_indices.add(media_idx)
                    logging.info(f"Matched unique group: {len(overlay_group)} overlays with {len(media_group)} media files")
    
    # Second pass: best-effort matching for non-unique counts
    for count in overlay_counts.keys():
        if count in media_counts:
            overlay_groups_with_count = overlay_counts[count]
            media_groups_with_count = media_counts[count]
            
            # Skip if already handled in first pass
            if len(overlay_groups_with_count) == 1 and len(media_groups_with_count) == 1:
                continue
            
            # Match remaining groups in order
            for overlay_idx, overlay_group in overlay_groups_with_count:
                if overlay_idx in used_overlay_indices:
                    continue
                    
                for media_idx, media_group in media_groups_with_count:
                    if media_idx in used_media_indices:
                        continue
                    
                    matched_pairs.append((overlay_group, media_group))
                    used_overlay_indices.add(overlay_idx)
                    used_media_indices.add(media_idx)
                    logging.warning(f"Best-effort match: {len(overlay_group)} overlays with {len(media_group)} media files (non-unique count)")
                    break
    
    # Log unmatched groups
    unmatched_overlays = len(overlay_group_list) - len(used_overlay_indices)
    unmatched_media = len(media_groups) - len(used_media_indices)
    if unmatched_overlays > 0 or unmatched_media > 0:
        logging.warning(f"Unmatched groups: {unmatched_overlays} overlay groups, {unmatched_media} media groups")
    
    return matched_pairs


def process_grouped_multipart_video(
    date_str: str, 
    media_files: List[Path], 
    overlay_files: List[Path], 
    temp_dir: Path
) -> Optional[str]:
    """
    Process a matched group of media files and overlays.
    Each media file gets merged with one overlay from the group.
    Returns the folder name if successful, None otherwise.
    """
    if len(media_files) != len(overlay_files):
        logging.error(f"Mismatch in group sizes: {len(media_files)} media files, {len(overlay_files)} overlay files")
        return None
    
    # Sort both lists to ensure consistent pairing
    media_files_sorted = sorted(media_files, key=lambda x: x.name)
    overlay_files_sorted = sorted(overlay_files, key=lambda x: x.name)
    
    # Use latest media file for folder naming
    latest_media = media_files_sorted[-1]
    folder_name = latest_media.stem + "_grouped"
    folder_path = temp_dir / folder_name
    
    try:
        ensure_directory(folder_path)
        
        timestamps = {}
        successful_parts = 0
        
        # Process each media-overlay pair
        for media_file, overlay_file in zip(media_files_sorted, overlay_files_sorted):
            output_filename = media_file.name
            output_path = folder_path / output_filename
            
            logging.info(f"Processing grouped pair: {media_file.name} + {overlay_file.name}")
            
            try:
                # Use the same FFmpeg command as other processing
                command = [
                    "ffmpeg",
                    "-y",
                    "-i", str(media_file),     # [0] base video
                    "-i", str(overlay_file),   # [1] overlay (alpha)
                    "-filter_complex",
                    "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
                    "-map", "[vout]",          # map filtered video
                    "-map", "0:a?",            # optional audio from base
                    "-map_metadata", "0",      # copy all tags from base
                    "-movflags", "+faststart",
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", "18",
                    "-c:a", "copy",
                    str(output_path),
                ]
                result = subprocess.run(command, capture_output=True, text=True)
                
                if result.returncode != 0:
                    logging.error(f"FFmpeg failed for {media_file.name}")
                    logging.error(f"FFmpeg stderr: {result.stderr}")
                    continue
                
                # FFmpeg succeeded
                successful_parts += 1
                
                # Extract timestamp for this part
                timestamp = extract_mp4_timestamp(media_file)
                if timestamp:
                    # Convert to ISO format
                    timestamp_seconds = timestamp / 1000
                    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    timestamps[output_filename] = dt.isoformat().replace('+00:00', 'Z')
                
                # Preserve file times
                try:
                    st = media_file.stat()
                    os.utime(output_path, (st.st_atime, st.st_mtime))
                except Exception as e:
                    logging.debug(f"Could not copy file times for {output_path.name}: {e}")
                    
            except Exception as e:
                logging.error(f"Error processing {media_file.name}: {e}")
                continue
        
        # Only create timestamps.json if we had successful merges
        if successful_parts > 0:
            timestamps_path = folder_path / "timestamps.json"
            with open(timestamps_path, 'w') as f:
                json.dump(timestamps, f, indent=2)
            
            logging.info(f"Created grouped multi-part video folder: {folder_name} with {successful_parts} successfully processed parts")
            return folder_name
        else:
            # No successful parts, clean up
            logging.error(f"No parts successfully processed for grouped multipart on {date_str}")
            if folder_path.exists():
                shutil.rmtree(folder_path)
            return None
        
    except Exception as e:
        logging.error(f"Error processing grouped multi-part video for {date_str}: {e}")
        if folder_path.exists():
            shutil.rmtree(folder_path)
        return None


def process_multipart_video(date_str: str, media_files: List[Path], overlay_file: Path, temp_dir: Path) -> Optional[str]:
    """
    Process multi-part videos by creating a folder and applying overlay to each part.
    Returns the folder name if successful, None otherwise.
    """
    # Sort media files by name to determine the latest part
    media_files_sorted = sorted(media_files, key=lambda x: x.name)
    latest_media = media_files_sorted[-1]
    
    # Create folder name based on latest video part (without extension)
    folder_name = latest_media.stem + "_multipart"
    folder_path = temp_dir / folder_name
    
    try:
        ensure_directory(folder_path)
        
        timestamps = {}
        successful_parts = 0
        
        for media_file in media_files_sorted:
            output_filename = media_file.name
            output_path = folder_path / output_filename
            
            logging.info(f"Processing part: {media_file.name}")
            
            try:
                # Use EXACTLY the same FFmpeg command as single file processing
                command = [
                    "ffmpeg",
                    "-y",
                    "-i", str(media_file),     # [0] base video
                    "-i", str(overlay_file),   # [1] overlay (alpha)
                    "-filter_complex",
                    "[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]",
                    "-map", "[vout]",          # map filtered video
                    "-map", "0:a?",            # optional audio from base
                    "-map_metadata", "0",      # copy all tags from base
                    "-movflags", "+faststart",
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", "18",
                    "-c:a", "copy",
                    str(output_path),
                ]
                result = subprocess.run(command, capture_output=True, text=True)
                
                if result.returncode != 0:
                    logging.error(f"FFmpeg failed for {media_file.name}")
                    logging.error(f"FFmpeg stderr: {result.stderr}")
                    continue
                
                # FFmpeg succeeded
                successful_parts += 1
                
                # Extract timestamp for this part
                timestamp = extract_mp4_timestamp(media_file)
                if timestamp:
                    # Convert to ISO format
                    timestamp_seconds = timestamp / 1000
                    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                    timestamps[output_filename] = dt.isoformat().replace('+00:00', 'Z')
                
                # Preserve file times
                try:
                    st = media_file.stat()
                    os.utime(output_path, (st.st_atime, st.st_mtime))
                except Exception as e:
                    logging.debug(f"Could not copy file times for {output_path.name}: {e}")
                    

            except Exception as e:
                logging.error(f"Error processing {media_file.name}: {e}")
                continue
        
        # Only create timestamps.json if we had successful merges
        if successful_parts > 0:
            timestamps_path = folder_path / "timestamps.json"
            with open(timestamps_path, 'w') as f:
                json.dump(timestamps, f, indent=2)
            
            logging.info(f"Created multi-part video folder: {folder_name} with {successful_parts} successfully processed parts")
            return folder_name
        else:
            # No successful parts, clean up
            logging.error(f"No parts successfully processed for {date_str}")
            if folder_path.exists():
                shutil.rmtree(folder_path)
            return None
        
    except Exception as e:
        logging.error(f"Error processing multi-part video for {date_str}: {e}")
        if folder_path.exists():
            shutil.rmtree(folder_path)
        return None


def merge_conversations(chat_data: Dict[str, List], snap_data: Dict[str, List]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Merges chat and snap histories into a single dictionary and sorts messages by timestamp.
    """
    logging.info("Merging chat and snap histories...")
    merged = {}

    # Process chat messages
    for conv_id, messages in chat_data.items():
        if conv_id not in merged:
            merged[conv_id] = []
        for msg in messages:
            msg["Type"] = "message"
        merged[conv_id].extend(messages)

    # Process snaps
    for conv_id, snaps in snap_data.items():
        if conv_id not in merged:
            merged[conv_id] = []
        for snap in snaps:
            snap["Type"] = "snap"
        merged[conv_id].extend(snaps)

    # Sort all conversations by timestamp
    for conv_id in merged:
        merged[conv_id].sort(key=lambda x: int(x.get("Created(microseconds)", 0)))

    logging.info(f"Merged {len(merged)} conversations.")
    return merged


def extract_media_id_from_filename(filename: str) -> Optional[str]:
    """
    Extracts the Media ID from a Snapchat media filename using regex patterns.
    """
    if 'thumbnail~' in filename.lower():
        return None
    # Pattern for 'b~...' IDs
    if 'b~' in filename:
        parts = filename.split('b~', 1)
        if len(parts) > 1:
            id_part = parts[1].rsplit('.', 1)[0]
            return f'b~{id_part}'
    # Pattern for 'media~zip-...' IDs
    match = re.search(r'media~zip-([A-F0-9\-]+)', filename, re.I)
    if match:
        return f'media~zip-{match.group(1)}'
    # General pattern for 'media~...' or 'overlay~...' IDs
    match = re.search(r'(media|overlay)~([A-F0-9\-]+)', filename, re.I)
    if match:
        return f'{match.group(1)}~{match.group(2)}'
    return None


def index_media_files(media_dir: Path, files_to_ignore: set) -> Dict[str, str]:
    """
    Creates a dictionary mapping Media IDs to their filenames or folder names.
    Handles both individual files and multi-part video folders.
    """
    logging.info(f"Indexing media files in {media_dir}...")
    media_index = {}
    if not media_dir.exists():
        logging.warning(f"Media directory not found: {media_dir}")
        return {}

    for item_path in media_dir.iterdir():
        if item_path.is_file() and item_path.name not in files_to_ignore:
            media_id = extract_media_id_from_filename(item_path.name)
            if media_id:
                media_index[media_id] = item_path.name
        elif item_path.is_dir() and (item_path.name.endswith("_multipart") or item_path.name.endswith("_grouped")):
            # For multi-part folders, index each file inside but map to the folder
            for file_path in item_path.iterdir():
                if file_path.is_file() and file_path.suffix.lower() == '.mp4':
                    media_id = extract_media_id_from_filename(file_path.name)
                    if media_id:
                        # Map to folder name, not individual file
                        media_index[media_id] = item_path.name
    
    logging.info(f"Indexed {len(media_index)} media files/folders with valid IDs.")
    return media_index


def extract_mp4_timestamp(mp4_path: Path) -> Optional[int]:
    """
    Extracts the creation timestamp from an MP4 file by parsing the 'moov' atom.
    Returns a Unix timestamp in milliseconds.
    """
    try:
        with open(mp4_path, "rb") as f:
            while True:
                atom_header = f.read(8)
                if not atom_header:
                    return None
                atom_size = struct.unpack('>I', atom_header[0:4])[0]
                atom_type = atom_header[4:8]

                if atom_type == b'moov':
                    mvhd_header = f.read(8)
                    if mvhd_header[4:8] == b'mvhd':
                        version = f.read(1)[0]
                        f.seek(3, 1)
                        if version == 0:
                            creation_time = struct.unpack('>I', f.read(4))[0]
                        else:
                            creation_time = struct.unpack('>Q', f.read(8))[0]

                        unix_timestamp = creation_time - QUICKTIME_EPOCH_ADJUSTER
                        return unix_timestamp * 1000
                    else:
                        return None

                if atom_size == 1:
                    extended_size = struct.unpack('>Q', f.read(8))[0]
                    f.seek(extended_size - 16, 1)
                else:
                    f.seek(atom_size - 8, 1)
    except Exception as e:
        logging.debug(f"Could not parse MP4 timestamp for {mp4_path.name}: {e}")
        return None


def determine_account_owner(conversations: Dict[str, List[Dict[str, Any]]]) -> str:
    """
    Determines the account owner's username by finding a message sent by them.
    """
    for messages in conversations.values():
        for msg in messages:
            if msg.get('IsSender'):
                owner = msg.get('From')
                if owner:
                    logging.info(f"Determined account owner: {owner}")
                    return owner
    logging.warning("Could not determine account owner. Defaulting to 'unknown'.")
    return "unknown"


def create_conversation_metadata(
    conv_id: str,
    messages: List[Dict[str, Any]],
    friends_details_map: Dict[str, Any],
    account_owner: str
) -> Dict[str, Any]:
    """
    Generates the detailed metadata object for a conversation.
    """
    is_group = any(msg.get("Conversation Title") for msg in messages)

    participant_usernames = set()
    for msg in messages:
        sender = msg.get("From")
        if sender:
            participant_usernames.add(sender)

    if not is_group:
        participant_usernames.add(conv_id)

    non_owner_participants = {p for p in participant_usernames if p != account_owner}

    participants_list = []
    for username in sorted(list(non_owner_participants)):
        friend_data = friends_details_map.get(username, {})
        participants_list.append({
            "username": username,
            "display_name": friend_data.get('Display Name', username),
            "creation_timestamp": friend_data.get('Creation Timestamp', 'N/A'),
            "last_modified_timestamp": friend_data.get('Last Modified Timestamp', 'N/A'),
            "source": friend_data.get('Source', 'unknown'),
            "friend_status": friend_data.get('friend_status', 'not_found'),
            "friend_list_section": friend_data.get('friend_list_section', 'Not Found'),
            "is_owner": False
        })

    metadata = {
        "conversation_type": "group" if is_group else "individual",
        "conversation_id": conv_id,
        "total_messages": len(messages),
        "snap_count": sum(1 for msg in messages if msg.get('Type') == 'snap'),
        "chat_count": sum(1 for msg in messages if msg.get('Type') == 'message'),
        "participants": participants_list,
        "participant_count": len(participants_list),
        "account_owner": account_owner,
        "date_range": {
            "first_message": messages[0].get("Created", "N/A") if messages else "N/A",
            "last_message": messages[-1].get("Created", "N/A") if messages else "N/A"
        },
        "index_created": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

    if is_group:
        metadata["group_name"] = messages[0].get("Conversation Title", conv_id)

    return metadata


def main():
    """
    Main function to run the entire Snapchat media mapping process.
    """
    logging.info("--- Starting Simplified Snapchat Media Mapper ---")
    
    # Check if output directory exists and remove it for a clean start
    if OUTPUT_DIR.exists():
        logging.info(f"Removing existing output directory: {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)
        logging.info("Output directory cleaned.")

    # --- Step 1: Find and Validate Input Data ---
    export_folders = [d for d in INPUT_DIR.iterdir() if d.is_dir() and (d / "json").exists() and (d / "chat_media").exists()]
    if not export_folders:
        logging.error(f"No valid Snapchat export folder found in '{INPUT_DIR}'.")
        logging.error("Please place your export folder (e.g., 'mydata') inside the 'input' directory.")
        return

    data_export_dir = export_folders[0]
    json_dir = data_export_dir / "json"
    source_media_dir = data_export_dir / "chat_media"
    logging.info(f"Processing data from: {data_export_dir}")

    temp_media_dir = OUTPUT_DIR / "temp_media"
    ensure_directory(temp_media_dir)

    # --- Step 1.5: Overlay Merging (now alpha/size-safe and ignores thumbnails) ---
    merged_source_files = merge_overlay_pairs(source_media_dir, temp_media_dir)

    # Copy all other non-merged files to the temp directory (excluding thumbnails)
    for item in source_media_dir.iterdir():
        if item.is_file() and item.name not in merged_source_files and "thumbnail" not in item.name.lower():
            shutil.copy(item, temp_media_dir / item.name)

    # --- Step 2: Load Data ---
    chat_history = load_json(json_dir / "chat_history.json")
    snap_history = load_json(json_dir / "snap_history.json")
    friends_data = load_json(json_dir / "friends.json")

    conversations = merge_conversations(chat_history, snap_history)
    account_owner = determine_account_owner(conversations)

    friends_details_map = {}
    for friend in friends_data.get('Friends', []):
        friend['friend_status'] = 'active'
        friend['friend_list_section'] = 'Friends'
        friends_details_map[friend['Username']] = friend
    for friend in friends_data.get('Deleted Friends', []):
        friend['friend_status'] = 'deleted'
        friend['friend_list_section'] = 'Deleted Friends'
        friends_details_map[friend['Username']] = friend

    # --- Step 3: Index Media Files ---
    media_index = index_media_files(temp_media_dir, set()) # Index all files in temp

    # --- Step 4: Perform Media Mapping ---
    logging.info("Starting media mapping process...")

    mapping: Dict[str, Dict[int, List[str]]] = {conv_id: {} for conv_id in conversations}
    mapped_files = set()

    # A. Media ID Mapping
    logging.info("Phase 1: Mapping via Media ID...")
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            media_ids_str = msg.get("Media IDs", "")
            if media_ids_str:
                media_ids = [mid.strip() for mid in media_ids_str.split('|')]
                for media_id in media_ids:
                    if media_id in media_index:
                        filename = media_index[media_id]
                        if i not in mapping[conv_id]:
                            mapping[conv_id][i] = []
                        mapping[conv_id][i].append(filename)
                        mapped_files.add(filename)

    logging.info(f"Mapped {len(mapped_files)} files using Media IDs.")

    # B. MP4 Timestamp Mapping (including multi-part folders)
    logging.info("Phase 2: Mapping MP4s and multi-part folders via Timestamps...")
    
    # Collect unmapped individual MP4s
    unmapped_mp4s = [f for f in temp_media_dir.iterdir() 
                     if f.is_file() and f.suffix.lower() == '.mp4' 
                     and f.name not in mapped_files]
    
    # Collect multi-part folders that haven't been mapped yet
    unmapped_folders = [f for f in temp_media_dir.iterdir() 
                       if f.is_dir() and (f.name.endswith("_multipart") or f.name.endswith("_grouped"))
                       and f.name not in mapped_files]

    mp4_timestamps: Dict[str, int] = {}
    folder_timestamps: Dict[str, Dict[str, int]] = {}
    
    # Process individual MP4s
    for mp4_file in unmapped_mp4s:
        timestamp = extract_mp4_timestamp(mp4_file)
        if timestamp:
            mp4_timestamps[mp4_file.name] = timestamp
    
    # Process multi-part folders
    for folder in unmapped_folders:
        timestamps_file = folder / "timestamps.json"
        if timestamps_file.exists():
            try:
                with open(timestamps_file, 'r') as f:
                    timestamps_data = json.load(f)
                    # Convert ISO timestamps to milliseconds
                    folder_timestamps[folder.name] = {}
                    for filename, iso_timestamp in timestamps_data.items():
                        dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
                        timestamp_ms = int(dt.timestamp() * 1000)
                        folder_timestamps[folder.name][filename] = timestamp_ms
            except Exception as e:
                logging.error(f"Error reading timestamps.json for {folder.name}: {e}")

    logging.info(f"Found {len(mp4_timestamps)} unmapped MP4s and {len(folder_timestamps)} unmapped folders to match.")

    all_messages_flat: List[Tuple[str, int, int]] = []
    for conv_id, messages in conversations.items():
        for i, msg in enumerate(messages):
            ts = int(msg.get("Created(microseconds)", 0))
            if ts > 0:
                all_messages_flat.append((conv_id, i, ts))

    all_messages_flat.sort(key=lambda x: x[2])

    mp4_matches = 0
    
    # Match individual MP4s
    for filename, mp4_ts in mp4_timestamps.items():
        best_match: Optional[Tuple[str, int, int]] = None
        min_diff = float('inf')

        for conv_id, msg_idx, msg_ts in all_messages_flat:
            diff = abs(mp4_ts - msg_ts)
            if diff < min_diff:
                min_diff = diff
                best_match = (conv_id, msg_idx, msg_ts)

        if best_match and min_diff <= (TIMESTAMP_THRESHOLD_SECONDS * 1000):
            conv_id, msg_idx, _ = best_match
            if msg_idx not in mapping[conv_id]:
                mapping[conv_id][msg_idx] = []
            mapping[conv_id][msg_idx].append(filename)
            mapped_files.add(filename)
            mp4_matches += 1
    
    # Match multi-part folders
    folder_matches = 0
    for folder_name, timestamps in folder_timestamps.items():
        best_match: Optional[Tuple[str, int, int]] = None
        min_diff = float('inf')
        
        # Check all timestamps in the folder to find the best match
        for filename, folder_ts in timestamps.items():
            for conv_id, msg_idx, msg_ts in all_messages_flat:
                diff = abs(folder_ts - msg_ts)
                if diff < min_diff:
                    min_diff = diff
                    best_match = (conv_id, msg_idx, msg_ts)
        
        if best_match and min_diff <= (TIMESTAMP_THRESHOLD_SECONDS * 1000):
            conv_id, msg_idx, _ = best_match
            if msg_idx not in mapping[conv_id]:
                mapping[conv_id][msg_idx] = []
            # Add the entire folder as a single unit
            mapping[conv_id][msg_idx].append(folder_name)
            mapped_files.add(folder_name)
            folder_matches += 1

    logging.info(f"Mapped {mp4_matches} MP4s and {folder_matches} multi-part folders using timestamps.")
    logging.info(f"Total unique files/folders mapped: {len(mapped_files)}")

    # --- Step 5: Organize Files and Generate Output ---
    logging.info("Organizing files and creating conversation.json for all conversations...")
    ensure_directory(OUTPUT_DIR)

    for conv_id, messages in conversations.items():
        if not messages:
            continue

        metadata = create_conversation_metadata(conv_id, messages, friends_details_map, account_owner)

        last_message_date = messages[-1].get("Created", "0000-00-00").split(" ")[0]

        folder_name_base = metadata.get("group_name")
        if not folder_name_base:
            if metadata['participants']:
                first_participant = metadata['participants'][0]
                folder_name_base = first_participant['display_name'] or first_participant['username']
            else:
                folder_name_base = conv_id

        folder_name = f"{last_message_date} - {folder_name_base}"
        sanitized_folder_name = re.sub(r'[\\/*?:"<>|]', "", folder_name)

        is_group = metadata['conversation_type'] == 'group'
        base_output_dir = OUTPUT_DIR / "groups" if is_group else OUTPUT_DIR / "conversations"
        conv_output_dir = base_output_dir / sanitized_folder_name
        ensure_directory(conv_output_dir)

        for msg_idx, items in mapping.get(conv_id, {}).items():
            if msg_idx < len(conversations[conv_id]):
                conversations[conv_id][msg_idx]["media_locations"] = [f"media/{item}" for item in items]
                for item_name in items:
                    media_output_dir = conv_output_dir / "media"
                    ensure_directory(media_output_dir)
                    source_path = temp_media_dir / item_name
                    dest_path = media_output_dir / item_name
                    
                    if source_path.exists() and not dest_path.exists():
                        if source_path.is_file():
                            # Copy individual file
                            shutil.copy(source_path, dest_path)
                        elif source_path.is_dir():
                            # Copy entire folder for multi-part videos
                            shutil.copytree(source_path, dest_path)

        save_json({
            "conversation_metadata": metadata,
            "messages": messages
        }, conv_output_dir / "conversation.json")

    # Handle Orphaned Media (files and folders)
    logging.info("Copying orphaned files and folders...")
    orphaned_dir = OUTPUT_DIR / "orphaned"
    ensure_directory(orphaned_dir)
    orphaned_count = 0
    
    for item_path in temp_media_dir.iterdir():
        if item_path.name not in mapped_files and "thumbnail" not in item_path.name.lower():
            if item_path.is_file():
                shutil.copy(str(item_path), str(orphaned_dir / item_path.name))
                orphaned_count += 1
            elif item_path.is_dir() and (item_path.name.endswith("_multipart") or item_path.name.endswith("_grouped")):
                # Copy entire orphaned multi-part folder
                shutil.copytree(str(item_path), str(orphaned_dir / item_path.name))
                orphaned_count += 1

    # Clean up the temporary media directory
    try:
        shutil.rmtree(temp_media_dir)
        logging.info("Cleaned up temporary media directory.")
    except OSError as e:
        logging.error(f"Error removing temporary directory {temp_media_dir}: {e}")

    logging.info(f"Copied {orphaned_count} orphaned files/folders.")
    logging.info(f"--- Process Complete! Check the '{OUTPUT_DIR}' directory. ---")


if __name__ == "__main__":
    main()
