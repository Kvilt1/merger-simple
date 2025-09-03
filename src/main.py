#!/usr/bin/env python3
"""Main orchestration for Snapchat media mapper."""

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Set

from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    ensure_directory,
    load_json,
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
    collect_all_usernames
)

from day_index_converter import convert_from_memory
from bitmoji_processing import extract_bitmojis

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


def main():
    """Main processing function."""
    import time
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description="Process Snapchat export data")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Input directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--no-clean", action="store_true", help="Don't clean output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--no-hash", action="store_true", 
                       help="Keep original filenames in media pool (disables deduplication)")
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
        friends_map, friend_usernames = process_friends_data(friends_json)
        account_owner = determine_account_owner(conversations)
        
        logger.info(f"Loaded {len(conversations)} conversations")
        phase_times['data_loading'] = time.time() - phase_start
        
        # BITMOJI EXTRACTION PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: BITMOJI EXTRACTION")
        logger.info("=" * 60)
        
        # Collect all unique usernames
        all_usernames = collect_all_usernames(conversations, friends_map)
        logger.info(f"Found {len(all_usernames)} unique usernames for Bitmoji extraction")
        
        # Extract Bitmojis for all users
        avatars = extract_bitmojis(all_usernames)
        all_stats['bitmojis'] = len(avatars)
        logger.info(f"Extracted {len(avatars)} Bitmojis/avatars")
        phase_times['bitmoji_extraction'] = time.time() - phase_start
        
        # MEDIA INDEXING AND MAPPING PHASE
        phase_start = time.time()
        media_index, index_stats = index_media_files(temp_media_dir)
        all_stats['index'] = index_stats
        
        mappings, _, mapping_stats = map_media_to_messages(conversations, media_index, temp_media_dir)
        all_stats['mapping'] = mapping_stats
        phase_times['media_mapping'] = time.time() - phase_start
        
        # OUTPUT ORGANIZATION PHASE
        phase_start = time.time()
        logger.info("=" * 60)
        logger.info("PHASE: OUTPUT ORGANIZATION")
        logger.info("=" * 60)
        
        ensure_directory(args.output)
        
        # Use the day-index format
        converter_stats = convert_from_memory(
            conversations=conversations,
            friends_map=friends_map,
            account_owner=account_owner,
            mappings=mappings,
            temp_media_dir=temp_media_dir,
            output_dir=args.output,
            use_hash=not args.no_hash,
            max_workers=4,
            avatars=avatars
        )
        
        # Add converter stats to all_stats
        all_stats['conversations'] = converter_stats.get('total_conversations', 0)
        all_stats['events'] = converter_stats.get('total_events', 0)
        all_stats['days'] = converter_stats.get('total_days', 0)
        all_stats['orphaned'] = converter_stats.get('orphaned_media', 0)
        
        logger.info(f"Organized {converter_stats['total_conversations']} conversations into {converter_stats['total_days']} days")
        phase_times['output_organization'] = time.time() - phase_start
        
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
        
        logger.info("")
        logger.info("Day-Index Output:")
        logger.info(f"  - Total conversations:             {all_stats.get('conversations', 0)}")
        logger.info(f"  - Total events:                    {all_stats.get('events', 0)}")
        logger.info(f"  - Days with activity:              {all_stats.get('days', 0)}")
        logger.info(f"  - Bitmojis/avatars extracted:      {all_stats.get('bitmojis', 0)}")
        
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