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

def copy_unmerged_files(source_dir: Path, temp_dir: Path, merged_files: Set[str]) -> None:
    """Copy non-merged files to temp directory."""
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

def handle_orphaned_media(temp_dir: Path, output_dir: Path, mapped_files: Set[str]) -> None:
    """Copy orphaned media to separate directory."""
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

def main():
    """Main processing function."""
    parser = argparse.ArgumentParser(description="Process Snapchat export data")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Input directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--no-clean", action="store_true", help="Don't clean output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    
    logger.info("--- Starting Snapchat Media Mapper ---")
    
    try:
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
        
        # Process overlays
        logger.info("Processing overlays...")
        merged_files = merge_overlay_pairs(source_media_dir, temp_media_dir)
        
        # Copy unmerged files
        copy_unmerged_files(source_media_dir, temp_media_dir, merged_files)
        
        # Load data
        logger.info("Loading data files...")
        chat_data = load_json(json_dir / "chat_history.json")
        snap_data = load_json(json_dir / "snap_history.json")
        friends_json = load_json(json_dir / "friends.json")
        
        if not chat_data and not snap_data:
            raise ValueError("No chat or snap data found")
        
        # Process conversations
        conversations = merge_conversations(chat_data, snap_data)
        friends_map = process_friends_data(friends_json)
        account_owner = determine_account_owner(conversations)
        
        # Index and map media
        media_index = index_media_files(temp_media_dir)
        mappings, mapped_files = map_media_to_messages(conversations, media_index, temp_media_dir)
        
        # Organize output
        logger.info("Organizing output...")
        ensure_directory(args.output)
        
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
        
        # Handle orphaned media
        logger.info("Processing orphaned media...")
        handle_orphaned_media(temp_media_dir, args.output, mapped_files)
        
        # Cleanup
        logger.info("Cleaning up...")
        if temp_media_dir.exists():
            shutil.rmtree(temp_media_dir)
            logger.info("Removed temporary directory")
        
        logger.info("--- Process Complete! ---")
        logger.info(f"Check '{args.output}' directory for results")
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())