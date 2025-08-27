#!/usr/bin/env python3
"""Optimized main orchestration for large Snapchat data dumps (2GB+)."""

import argparse
import logging
import shutil
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Set, Tuple, Optional
from tqdm import tqdm
import psutil

# Import original modules where optimization isn't critical
from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    ensure_directory,
    save_json,
    sanitize_filename,
    logger
)

# Import optimized modules
from json_streaming import (
    load_chat_history_streaming,
    load_snap_history_streaming,
    load_friends_streaming,
    count_conversations_streaming
)

from media_processing_optimized import (
    merge_overlay_pairs_parallel,
    index_media_files_optimized,
    map_media_to_messages_optimized
)

from conversation import (
    determine_account_owner,
    create_conversation_metadata,
    get_conversation_folder_name
)

from optimization_utils import (
    get_optimal_worker_count,
    get_memory_limit,
    should_use_parallel
)

class OptimizedProcessor:
    """Optimized processor for large Snapchat data dumps."""
    
    def __init__(self, input_dir: Path, output_dir: Path, args):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.args = args
        self.stats = {
            'start_time': time.time(),
            'conversations_processed': 0,
            'media_files_processed': 0,
            'memory_peak': 0
        }
        
    def find_export_folder(self) -> Path:
        """Find Snapchat export folder."""
        for d in self.input_dir.iterdir():
            if d.is_dir() and (d / "json").exists() and (d / "chat_media").exists():
                return d
        
        raise FileNotFoundError(
            f"No valid Snapchat export found in '{self.input_dir}'. "
            "Place your export folder (e.g., 'mydata') inside 'input' directory."
        )
    
    def process_conversation_batch(self, batch: list) -> int:
        """Process a batch of conversations."""
        processed = 0
        for conv_id, messages, metadata, folder_name, conv_dir, mapping in batch:
            try:
                ensure_directory(conv_dir)
                
                # Process media if mapping exists
                if mapping:
                    self.process_conversation_media_optimized(
                        conv_id, messages, mapping, conv_dir
                    )
                
                # Save conversation data
                save_json({
                    "conversation_metadata": metadata,
                    "messages": messages
                }, conv_dir / "conversation.json")
                
                processed += 1
            except Exception as e:
                logger.error(f"Error processing conversation {conv_id}: {e}")
        
        return processed
    
    def process_conversation_media_optimized(self, conv_id: str, messages: list, 
                                            mapping: dict, conv_dir: Path) -> None:
        """Optimized media copying with parallel I/O."""
        if not mapping:
            return
        
        media_dir = conv_dir / "media"
        ensure_directory(media_dir)
        
        # Prepare copy tasks
        copy_tasks = []
        temp_media_dir = self.export_dir / "chat_media"  # Store ref for lambda
        
        for msg_idx, items in mapping.items():
            if msg_idx >= len(messages):
                continue
            
            for item in items:
                filename = item["filename"]
                source = self.temp_media_dir / filename
                dest = media_dir / filename
                
                if source.exists() and not dest.exists():
                    copy_tasks.append((source, dest, msg_idx, item))
        
        # Execute copies in parallel with thread pool (I/O bound)
        if copy_tasks and should_use_parallel(len(copy_tasks), 0):
            with ThreadPoolExecutor(max_workers=get_optimal_worker_count("io")) as executor:
                futures = []
                for source, dest, msg_idx, item in copy_tasks:
                    future = executor.submit(self._copy_media_file, source, dest)
                    futures.append((future, msg_idx, item))
                
                for future, msg_idx, item in futures:
                    try:
                        success = future.result(timeout=30)
                        if success:
                            # Update message metadata
                            if "media_locations" not in messages[msg_idx]:
                                messages[msg_idx]["media_locations"] = []
                            
                            location = f"media/{item['filename']}"
                            messages[msg_idx]["media_locations"].append(location)
                            messages[msg_idx]["mapping_method"] = item.get("mapping_method")
                    except Exception as e:
                        logger.error(f"Failed to copy media: {e}")
        else:
            # Fallback to sequential for small batches
            for source, dest, msg_idx, item in copy_tasks:
                if self._copy_media_file(source, dest):
                    if "media_locations" not in messages[msg_idx]:
                        messages[msg_idx]["media_locations"] = []
                    location = f"media/{item['filename']}"
                    messages[msg_idx]["media_locations"].append(location)
    
    def _copy_media_file(self, source: Path, dest: Path) -> bool:
        """Copy a single media file."""
        try:
            if source.is_file():
                shutil.copy2(source, dest)
            elif source.is_dir():
                shutil.copytree(source, dest)
            return True
        except Exception as e:
            logger.error(f"Error copying {source.name}: {e}")
            return False
    
    def handle_orphaned_media_parallel(self, mapped_files: Set[str]) -> None:
        """Handle orphaned media with parallel copying."""
        orphaned_dir = self.output_dir / "orphaned"
        ensure_directory(orphaned_dir)
        
        orphaned_items = []
        
        for item in self.temp_media_dir.iterdir():
            if item.name in mapped_files:
                continue
            if "thumbnail" in item.name.lower():
                continue
            if "_overlay~" in item.name:
                continue
            
            orphaned_items.append(item)
        
        if orphaned_items:
            logger.info(f"Processing {len(orphaned_items)} orphaned items")
            
            with ThreadPoolExecutor(max_workers=get_optimal_worker_count("io")) as executor:
                with tqdm(total=len(orphaned_items), desc="Copying orphaned media") as pbar:
                    futures = []
                    for item in orphaned_items:
                        dest = orphaned_dir / item.name
                        future = executor.submit(self._copy_media_file, item, dest)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        pbar.update(1)
    
    def run(self):
        """Main processing pipeline with optimizations."""
        logger.info("--- Starting Optimized Snapchat Media Mapper ---")
        logger.info(f"System: {os.cpu_count()} CPUs, {psutil.virtual_memory().total / (1024**3):.1f}GB RAM")
        
        try:
            # Find export folder
            self.export_dir = self.find_export_folder()
            json_dir = self.export_dir / "json"
            source_media_dir = self.export_dir / "chat_media"
            self.temp_media_dir = self.output_dir / "temp_media"
            
            logger.info(f"Processing export from: {self.export_dir}")
            
            # Count total conversations for progress tracking
            chat_count = count_conversations_streaming(json_dir / "chat_history.json")
            snap_count = count_conversations_streaming(json_dir / "snap_history.json")
            total_conversations = chat_count + snap_count
            logger.info(f"Found {total_conversations} conversations to process")
            
            # Process overlays in parallel
            logger.info("Processing overlays with parallel FFmpeg...")
            merged_files = merge_overlay_pairs_parallel(source_media_dir, self.temp_media_dir)
            
            # Copy unmerged files (can be parallelized if needed)
            logger.info("Copying unmerged media files...")
            self._copy_unmerged_files_parallel(source_media_dir, self.temp_media_dir, merged_files)
            
            # Load friends data
            logger.info("Loading friends data...")
            friends_map = load_friends_streaming(json_dir / "friends.json")
            
            # Index media files
            logger.info("Indexing media files...")
            media_index = index_media_files_optimized(self.temp_media_dir)
            
            # Process conversations in streaming fashion
            logger.info("Processing conversations with streaming...")
            all_conversations = {}
            account_owner = None
            
            # Stream chat history
            with tqdm(total=total_conversations, desc="Loading conversations") as pbar:
                for conv_id, messages in load_chat_history_streaming(json_dir / "chat_history.json"):
                    for msg in messages:
                        msg["Type"] = "message"
                    all_conversations[conv_id] = messages
                    
                    # Determine owner from first conversation with sender
                    if not account_owner:
                        for msg in messages:
                            if msg.get("IsSender"):
                                account_owner = msg.get("From")
                                if account_owner:
                                    logger.info(f"Determined account owner: {account_owner}")
                                    break
                    pbar.update(1)
                
                # Stream snap history
                for conv_id, snaps in load_snap_history_streaming(json_dir / "snap_history.json"):
                    for snap in snaps:
                        snap["Type"] = "snap"
                    
                    if conv_id in all_conversations:
                        all_conversations[conv_id].extend(snaps)
                        all_conversations[conv_id].sort(key=lambda x: int(x.get("Created(microseconds)", 0)))
                    else:
                        all_conversations[conv_id] = snaps
                    pbar.update(1)
            
            if not account_owner:
                account_owner = "unknown"
            
            # Map media to messages
            logger.info("Mapping media to messages with optimized algorithms...")
            mappings, mapped_files = map_media_to_messages_optimized(
                all_conversations, media_index, self.temp_media_dir
            )
            
            # Process conversations with batching
            logger.info("Writing output with parallel I/O...")
            ensure_directory(self.output_dir)
            
            conversation_batch = []
            batch_size = 10
            
            with tqdm(total=len(all_conversations), desc="Processing conversations") as pbar:
                for conv_id, messages in all_conversations.items():
                    if not messages:
                        pbar.update(1)
                        continue
                    
                    # Create metadata
                    metadata = create_conversation_metadata(conv_id, messages, friends_map, account_owner)
                    
                    # Create output directory
                    folder_name = get_conversation_folder_name(metadata, messages)
                    folder_name = sanitize_filename(folder_name)
                    
                    is_group = metadata["conversation_type"] == "group"
                    base_dir = self.output_dir / "groups" if is_group else self.output_dir / "conversations"
                    conv_dir = base_dir / folder_name
                    
                    # Add to batch
                    conversation_batch.append((
                        conv_id, messages, metadata, folder_name, conv_dir,
                        mappings.get(conv_id, {})
                    ))
                    
                    # Process batch when full
                    if len(conversation_batch) >= batch_size:
                        processed = self.process_conversation_batch(conversation_batch)
                        self.stats['conversations_processed'] += processed
                        conversation_batch = []
                    
                    pbar.update(1)
                
                # Process remaining batch
                if conversation_batch:
                    processed = self.process_conversation_batch(conversation_batch)
                    self.stats['conversations_processed'] += processed
            
            # Handle orphaned media
            logger.info("Processing orphaned media...")
            self.handle_orphaned_media_parallel(mapped_files)
            
            # Cleanup
            logger.info("Cleaning up...")
            if self.temp_media_dir.exists():
                shutil.rmtree(self.temp_media_dir)
                logger.info("Removed temporary directory")
            
            # Print statistics
            elapsed = time.time() - self.stats['start_time']
            logger.info("--- Process Complete! ---")
            logger.info(f"Total time: {elapsed:.1f} seconds")
            logger.info(f"Conversations processed: {self.stats['conversations_processed']}")
            logger.info(f"Peak memory usage: {psutil.Process().memory_info().rss / (1024**3):.2f}GB")
            logger.info(f"Check '{self.output_dir}' directory for results")
            
            return 0
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return 1
    
    def _copy_unmerged_files_parallel(self, source_dir: Path, temp_dir: Path, merged_files: Set[str]):
        """Parallel version of copying unmerged files."""
        copy_tasks = []
        
        for item in source_dir.iterdir():
            if not item.is_file():
                continue
            
            # Skip merged files, thumbnails, and overlays
            if item.name in merged_files:
                continue
            if "thumbnail" in item.name.lower():
                continue
            if "_overlay~" in item.name:
                continue
            
            copy_tasks.append((item, temp_dir / item.name))
        
        if copy_tasks:
            logger.info(f"Copying {len(copy_tasks)} unmerged files")
            
            with ThreadPoolExecutor(max_workers=get_optimal_worker_count("io")) as executor:
                with tqdm(total=len(copy_tasks), desc="Copying unmerged files") as pbar:
                    futures = []
                    for source, dest in copy_tasks:
                        future = executor.submit(shutil.copy2, source, dest)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        pbar.update(1)

def main():
    """Main entry point for optimized processor."""
    parser = argparse.ArgumentParser(description="Optimized Snapchat export processor for large datasets")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Input directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--no-clean", action="store_true", help="Don't clean output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--workers", type=int, help="Override worker count")
    parser.add_argument("--memory-limit", type=int, help="Memory limit in MB")
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    
    # Clean output if requested
    if not args.no_clean and args.output.exists():
        logger.info(f"Cleaning output directory: {args.output}")
        shutil.rmtree(args.output)
    
    # Run optimized processor
    processor = OptimizedProcessor(args.input, args.output, args)
    sys.exit(processor.run())

if __name__ == "__main__":
    main()