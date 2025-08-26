"""Conversation processing and metadata generation."""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set

logger = logging.getLogger(__name__)

def merge_conversations(chat_data: Dict, snap_data: Dict) -> Dict[str, List]:
    """Merge chat and snap histories."""
    logger.info("Merging chat and snap histories")
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
    
    # Sort by timestamp
    for conv_id in merged:
        merged[conv_id].sort(key=lambda x: int(x.get("Created(microseconds)", 0)))
    
    logger.info(f"Merged {len(merged)} conversations")
    return merged

def process_friends_data(friends_json: Dict) -> Dict[str, Dict]:
    """Process friends data."""
    logger.info("Processing friends data")
    friends_map = {}
    
    # Active friends
    for friend in friends_json.get("Friends", []):
        friend["friend_status"] = "active"
        friend["friend_list_section"] = "Friends"
        friends_map[friend["Username"]] = friend
    
    # Deleted friends
    for friend in friends_json.get("Deleted Friends", []):
        friend["friend_status"] = "deleted"
        friend["friend_list_section"] = "Deleted Friends"
        friends_map[friend["Username"]] = friend
    
    logger.info(f"Processed {len(friends_map)} friends")
    return friends_map

def determine_account_owner(conversations: Dict[str, List]) -> str:
    """Determine account owner from messages."""
    for messages in conversations.values():
        for msg in messages:
            if msg.get("IsSender"):
                owner = msg.get("From")
                if owner:
                    logger.info(f"Determined account owner: {owner}")
                    return owner
    
    logger.warning("Could not determine account owner")
    return "unknown"

def get_conversation_participants(messages: List[Dict], conv_id: str, owner: str) -> Set[str]:
    """Get all participants in a conversation."""
    participants = set()
    is_group = any(msg.get("Conversation Title") for msg in messages)
    
    for msg in messages:
        sender = msg.get("From")
        if sender:
            participants.add(sender)
    
    # Add conversation ID for individual chats
    if not is_group:
        participants.add(conv_id)
    
    # Remove owner
    participants.discard(owner)
    return participants

def create_conversation_metadata(conv_id: str, messages: List[Dict], 
                                friends_map: Dict, owner: str) -> Dict:
    """Create metadata for a conversation."""
    is_group = any(msg.get("Conversation Title") for msg in messages)
    participants = get_conversation_participants(messages, conv_id, owner)
    
    # Build participant list
    participants_list = []
    for username in sorted(participants):
        friend = friends_map.get(username, {})
        participants_list.append({
            "username": username,
            "display_name": friend.get("Display Name", username),
            "creation_timestamp": friend.get("Creation Timestamp", "N/A"),
            "last_modified_timestamp": friend.get("Last Modified Timestamp", "N/A"),
            "source": friend.get("Source", "unknown"),
            "friend_status": friend.get("friend_status", "not_found"),
            "friend_list_section": friend.get("friend_list_section", "Not Found"),
            "is_owner": False
        })
    
    # Create metadata
    metadata = {
        "conversation_type": "group" if is_group else "individual",
        "conversation_id": conv_id,
        "total_messages": len(messages),
        "snap_count": sum(1 for msg in messages if msg.get("Type") == "snap"),
        "chat_count": sum(1 for msg in messages if msg.get("Type") == "message"),
        "participants": participants_list,
        "participant_count": len(participants_list),
        "account_owner": owner,
        "date_range": {
            "first_message": messages[0].get("Created", "N/A") if messages else "N/A",
            "last_message": messages[-1].get("Created", "N/A") if messages else "N/A"
        },
        "index_created": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }
    
    # Add group name if applicable
    if is_group:
        for msg in messages:
            if msg.get("Conversation Title"):
                metadata["group_name"] = msg["Conversation Title"]
                break
    
    return metadata

def get_conversation_folder_name(metadata: Dict, messages: List[Dict]) -> str:
    """Generate folder name for conversation."""
    # Get last message date
    last_date = "0000-00-00"
    if messages:
        last_date = messages[-1].get("Created", "0000-00-00").split(" ")[0]
    
    # Determine base name
    base_name = metadata.get("group_name")
    if not base_name:
        if metadata["participants"]:
            first = metadata["participants"][0]
            base_name = first.get("display_name") or first.get("username")
        else:
            base_name = metadata["conversation_id"]
    
    return f"{last_date} - {base_name}"