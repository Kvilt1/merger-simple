"""Streaming JSON parser for memory-efficient processing of large files."""

import ijson
import json
import logging
from pathlib import Path
from typing import Dict, Generator, Any, List
from collections import defaultdict

logger = logging.getLogger(__name__)

def stream_json_objects(file_path: Path, prefix: str = '') -> Generator[Dict, None, None]:
    """Stream JSON objects from a file without loading entire file into memory."""
    try:
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, prefix)
            for obj in parser:
                yield obj
    except Exception as e:
        logger.error(f"Error streaming {file_path}: {e}")
        return

def load_chat_history_streaming(file_path: Path) -> Generator[tuple[str, List[Dict]], None, None]:
    """Stream chat history conversations one at a time."""
    if not file_path.exists():
        logger.error(f"Chat history file not found: {file_path}")
        return
    
    try:
        with open(file_path, 'rb') as f:
            # Chat history is typically {"conversation_id": [messages...]}
            parser = ijson.kvitems(f, '')
            for conv_id, messages in parser:
                yield conv_id, messages
    except Exception as e:
        logger.error(f"Failed to stream chat history: {e}")

def load_snap_history_streaming(file_path: Path) -> Generator[tuple[str, List[Dict]], None, None]:
    """Stream snap history entries one at a time."""
    if not file_path.exists():
        logger.error(f"Snap history file not found: {file_path}")
        return
    
    try:
        with open(file_path, 'rb') as f:
            parser = ijson.kvitems(f, '')
            for snap_id, snaps in parser:
                yield snap_id, snaps
    except Exception as e:
        logger.error(f"Failed to stream snap history: {e}")

def load_friends_streaming(file_path: Path) -> Dict[str, Dict]:
    """Load friends data with streaming (still builds dict as it's typically smaller)."""
    friends_map = {}
    
    if not file_path.exists():
        logger.warning(f"Friends file not found: {file_path}")
        return friends_map
    
    try:
        with open(file_path, 'rb') as f:
            # Process active friends
            for friend in ijson.items(f, 'Friends.item'):
                friend["friend_status"] = "active"
                friend["friend_list_section"] = "Friends"
                friends_map[friend.get("Username", "")] = friend
            
            # Reset file pointer and process deleted friends
            f.seek(0)
            for friend in ijson.items(f, 'Deleted Friends.item'):
                friend["friend_status"] = "deleted"
                friend["friend_list_section"] = "Deleted Friends"
                friends_map[friend.get("Username", "")] = friend
                
    except Exception as e:
        logger.error(f"Failed to load friends data: {e}")
        # Fallback to regular JSON loading if streaming fails
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return process_friends_fallback(data)
        except:
            pass
    
    logger.info(f"Loaded {len(friends_map)} friends via streaming")
    return friends_map

def process_friends_fallback(friends_json: Dict) -> Dict[str, Dict]:
    """Fallback processing for friends data."""
    friends_map = {}
    
    for friend in friends_json.get("Friends", []):
        friend["friend_status"] = "active"
        friend["friend_list_section"] = "Friends"
        friends_map[friend["Username"]] = friend
    
    for friend in friends_json.get("Deleted Friends", []):
        friend["friend_status"] = "deleted"
        friend["friend_list_section"] = "Deleted Friends"
        friends_map[friend["Username"]] = friend
    
    return friends_map

def count_conversations_streaming(file_path: Path) -> int:
    """Count number of conversations without loading entire file."""
    count = 0
    try:
        with open(file_path, 'rb') as f:
            parser = ijson.kvitems(f, '')
            for _ in parser:
                count += 1
    except:
        pass
    return count