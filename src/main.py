#!/usr/bin/env python3
"""Main orchestration for Snapchat media mapper."""

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Dict, Set

from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    ensure_directory,
    load_json,
    save_json,
    sanitize_filename,
    logger
)

from media_processing import (
    merge_overlay_pairs,
    index_media_files,
    map_media_to_messages
)

from conversation import (
    merge_conversations,
    process_friends_data,
    determine_account_owner,
    create_conversation_metadata,
    get_conversation_folder_name
)

def find_export_folder(input_dir: Path) -> Path:
    """Find Snapchat export folder."""
    for d in input_dir.iterdir():
        if d.is_dir() and (d / "json").exists() and (d / "chat_media").exists():
            return d
    
    raise FileNotFoundError(
        f"No valid Snapchat export found in '{input_dir}'. "
        "Place your export folder (e.g., 'mydata') inside 'input' directory."
    )

def copy_unmerged_files(source_dir: Path, temp_dir: Path, merged_files: Set[str]) -> int:
    """Copy non-merged files to temp directory. Returns count of copied files."""
    copied = 0
    skipped_overlays = 0
    
    for item in source_dir.iterdir():
        if not item.is_file():
            continue
        
        # Skip merged files, thumbnails, and overlays
        if item.name in merged_files:
            continue
        if "thumbnail" in item.name.lower():
            continue
        if "_overlay~" in item.name:
            skipped_overlays += 1
            continue
        
        shutil.copy(item, temp_dir / item.name)
        copied += 1
    
    logger.info(f"Copied {copied} unmerged files")
    if skipped_overlays:
        logger.info(f"Skipped {skipped_overlays} overlay files")
    
    return copied

def process_conversation_media(conv_id: str, messages: list, mapping: dict, 
                             conv_dir: Path, temp_dir: Path) -> None:
    """Copy media files for a conversation."""
    if not mapping:
        return
    
    media_dir = conv_dir / "media"
    ensure_directory(media_dir)
    
    for msg_idx, items in mapping.items():
        if msg_idx >= len(messages):
            continue
        
        media_locations = []
        matched_files = []
        
        for item in items:
            filename = item["filename"]
            is_grouped = item.get("is_grouped", False)
            
            source = temp_dir / filename
            dest = media_dir / filename
            
            if source.exists() and not dest.exists():
                try:
                    if source.is_file():
                        shutil.copy(source, dest)
                        matched_files.append(filename)
                    elif source.is_dir():
                        shutil.copytree(source, dest)
                        # List files in folder
                        for f in source.iterdir():
                            if f.is_file() and f.name != "timestamps.json":
                                matched_files.append(f.name)
                except Exception as e:
                    logger.error(f"Error copying {filename}: {e}")
            
            # Add location reference
            location = f"media/{filename}/" if is_grouped else f"media/{filename}"
            media_locations.append(location)
        
        # Update message
        messages[msg_idx]["media_locations"] = media_locations
        messages[msg_idx]["matched_media_files"] = matched_files
        messages[msg_idx]["is_grouped"] = items[0].get("is_grouped", False)
        messages[msg_idx]["mapping_method"] = items[0].get("mapping_method")
        
        time_diff = items[0].get("time_diff_seconds")
        if time_diff is not None:
            messages[msg_idx]["time_diff_seconds"] = time_diff

def handle_orphaned_media(temp_dir: Path, output_dir: Path, mapped_files: Set[str]) -> int:
    """Copy orphaned media to separate directory. Returns count of orphaned items."""
    orphaned_dir = output_dir / "orphaned"
    ensure_directory(orphaned_dir)
    
    orphaned_count = 0
    
    for item in temp_dir.iterdir():
        # Skip mapped files, thumbnails, and overlays
        if item.name in mapped_files:
            continue
        if "thumbnail" in item.name.lower():
            continue
        if "_overlay~" in item.name:
            continue
        
        try:
            if item.is_file():
                shutil.copy(item, orphaned_dir / item.name)
                orphaned_count += 1
            elif item.is_dir() and (item.name.endswith("_multipart") or item.name.endswith("_grouped")):
                shutil.copytree(item, orphaned_dir / item.name)
                orphaned_count += 1
        except Exception as e:
            logger.error(f"Error copying orphaned {item.name}: {e}")
    
    logger.info(f"Copied {orphaned_count} orphaned items")
    return orphaned_count

def main():
    """Main processing function."""
    import time
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Process Snapchat export data")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Input directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--no-clean", action="store_true", help="Don't clean output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    
    logger.info("=" * 60)
    logger.info("    SNAPCHAT MEDIA MAPPER - STARTING")
    logger.info("=" * 60)
    
    # Store statistics for final summary
    all_stats = {}
    phase_times = {}
    
    try:
        # INITIALIZATION PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: INITIALIZATION")
        logger.info("=" * 60)
        
        # Clean output if requested
        if not args.no_clean and args.output.exists():
            logger.info(f"Cleaning output directory: {args.output}")
            shutil.rmtree(args.output)
        
        # Find export folder
        export_dir = find_export_folder(args.input)
        json_dir = export_dir / "json"
        source_media_dir = export_dir / "chat_media"
        temp_media_dir = args.output / "temp_media"
        
        logger.info(f"Processing export from: {export_dir}")
        phase_times['initialization'] = time.time() - phase_start
        
        # OVERLAY MERGING PHASE
        phase_start = time.time()
        merged_files, merge_stats = merge_overlay_pairs(source_media_dir, temp_media_dir)
        all_stats['merge'] = merge_stats
        phase_times['overlay_merging'] = time.time() - phase_start
        
        # COPY UNMERGED FILES
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: COPYING UNMERGED FILES")
        logger.info("=" * 60)
        
        copied_count = copy_unmerged_files(source_media_dir, temp_media_dir, merged_files)
        all_stats['copied_files'] = copied_count
        logger.info(f"Copied {copied_count} unmerged files")
        phase_times['copying'] = time.time() - phase_start
        
        # DATA LOADING PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: DATA LOADING AND PROCESSING")
        logger.info("=" * 60)
        
        chat_data = load_json(json_dir / "chat_history.json")
        snap_data = load_json(json_dir / "snap_history.json")
        friends_json = load_json(json_dir / "friends.json")
        
        if not chat_data and not snap_data:
            raise ValueError("No chat or snap data found")
        
        # Process conversations
        conversations = merge_conversations(chat_data, snap_data)
        friends_map = process_friends_data(friends_json)
        account_owner = determine_account_owner(conversations)
        
        logger.info(f"Loaded {len(conversations)} conversations")
        phase_times['data_loading'] = time.time() - phase_start
        
        # MEDIA INDEXING AND MAPPING PHASE
        phase_start = time.time()
        media_index, index_stats = index_media_files(temp_media_dir)
        all_stats['index'] = index_stats
        
        mappings, mapped_files, mapping_stats = map_media_to_messages(conversations, media_index, temp_media_dir)
        all_stats['mapping'] = mapping_stats
        phase_times['media_mapping'] = time.time() - phase_start
        
        # OUTPUT ORGANIZATION PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: OUTPUT ORGANIZATION")
        logger.info("=" * 60)
        
        ensure_directory(args.output)
        
        conversation_count = 0
        for conv_id, messages in conversations.items():
            if not messages:
                continue
            
            # Create metadata
            metadata = create_conversation_metadata(conv_id, messages, friends_map, account_owner)
            
            # Create output directory
            folder_name = get_conversation_folder_name(metadata, messages)
            folder_name = sanitize_filename(folder_name)
            
            is_group = metadata["conversation_type"] == "group"
            base_dir = args.output / "groups" if is_group else args.output / "conversations"
            conv_dir = base_dir / folder_name
            ensure_directory(conv_dir)
            
            # Process media
            process_conversation_media(
                conv_id, messages, mappings.get(conv_id, {}), 
                conv_dir, temp_media_dir
            )
            
            # Save conversation data
            save_json({
                "conversation_metadata": metadata,
                "messages": messages
            }, conv_dir / "conversation.json")
            
            conversation_count += 1
        
        logger.info(f"Organized {conversation_count} conversations")
        phase_times['output_organization'] = time.time() - phase_start
        
        # ORPHANED MEDIA PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: ORPHANED MEDIA PROCESSING")
        logger.info("=" * 60)
        
        orphaned_count = handle_orphaned_media(temp_media_dir, args.output, mapped_files)
        all_stats['orphaned'] = orphaned_count
        logger.info(f"Processed {orphaned_count} orphaned media files")
        phase_times['orphaned_processing'] = time.time() - phase_start
        
        # CLEANUP PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: CLEANUP")
        logger.info("=" * 60)
        
        if temp_media_dir.exists():
            shutil.rmtree(temp_media_dir)
            logger.info("Removed temporary directory")
        phase_times['cleanup'] = time.time() - phase_start
        
        # FINAL SUMMARY
        total_time = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info("         PROCESSING COMPLETE - SUMMARY")
        logger.info("=" * 60)
        
        # Calculate totals
        total_media_discovered = all_stats['merge'].get('total_media', 0) + all_stats['merge'].get('total_overlay', 0)
        total_processed = all_stats['merge'].get('total_merged', 0) + all_stats.get('copied_files', 0)
        
        total_mapped = all_stats['mapping'].get('mapped_by_id', 0) + all_stats['mapping'].get('mapped_by_timestamp', 0)
        
        logger.info(f"Total media files discovered:        {total_media_discovered}")
        logger.info(f"Successfully processed:              {total_processed}")
        if total_media_discovered > 0:
            process_pct = (total_processed / total_media_discovered) * 100
            logger.info(f"  - Processing rate:                 {process_pct:.1f}%")
        logger.info(f"  - Merged with overlays:            {all_stats['merge'].get('total_merged', 0)}")
        logger.info(f"  - Copied without overlays:         {all_stats.get('copied_files', 0)}")
        
        logger.info("")
        logger.info("Mapping Results:")
        logger.info(f"  - Mapped by Media ID:              {all_stats['mapping'].get('mapped_by_id', 0)}")
        logger.info(f"  - Mapped by timestamp:             {all_stats['mapping'].get('mapped_by_timestamp', 0)}")
        logger.info(f"  - Total mapped:                    {total_mapped}")
        logger.info(f"  - Orphaned (unmapped):             {all_stats.get('orphaned', 0)}")
        
        if total_processed > 0:
            map_pct = (total_mapped / total_processed) * 100
            logger.info(f"  - Mapping success rate:            {map_pct:.1f}%")
        
        logger.info("")
        logger.info("Processing Time:")
        for phase, duration in phase_times.items():
            logger.info(f"  - {phase.replace('_', ' ').title():<30} {duration:.1f}s")
        logger.info(f"  - {'Total':<30} {total_time:.1f}s")
        
        logger.info("=" * 60)
        logger.info(f"✓ Check '{args.output}' directory for results")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"ERROR: {e}")
        logger.error("=" * 60)
        return 1

if __name__ == "__main__":
    sys.exit(main())