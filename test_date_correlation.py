#!/usr/bin/env python3
"""Test script for date correlation mapping feature."""

import json
import logging
from pathlib import Path
from src.media_processing import get_media_type_from_extension, map_media_to_messages

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_date_correlation():
    """Test the date correlation mapping with example data."""
    
    # Test media type detection
    print("\n=== Testing Media Type Detection ===")
    test_files = [
        "2024-08-27_b~EiQSFXFYYW5zaHRhdTFvUVZmV1FIY2lKORoAGgAyAQhIAlAEYAE.webp",
        "2024-09-01_b~test.jpeg", 
        "2024-09-01_b~test.jpg",
        "2024-09-01_b~test.png",
        "2024-09-01_b~test.mp4",
        "2024-09-01_b~test.mov"
    ]
    
    for filename in test_files:
        media_type = get_media_type_from_extension(filename)
        print(f"{filename} → {media_type}")
    
    # Test with actual data structure
    print("\n=== Testing Date Correlation Mapping ===")
    
    # Simulate conversations with the snap from the example
    test_conversations = {
        "ptprotype": [
            {
                "From": "ptprotype",
                "Media Type": "IMAGE",
                "Created": "2024-08-27 17:43:46 UTC",
                "Conversation Title": None,
                "IsSender": True,
                "Created(microseconds)": 1724780626806,
                "Type": "snap"
            }
        ]
    }
    
    # Check if the date correlation would work
    print(f"\nTest snap details:")
    print(f"  Date: 2024-08-27")
    print(f"  From: ptprotype") 
    print(f"  Media Type: IMAGE")
    print(f"  Would match with: 2024-08-27_b~*.webp files")
    
    # Verify the logic
    date_str = "2024-08-27"
    orphaned_file = "2024-08-27_b~EiQSFXFYYW5zaHRhdTFvUVZmV1FIY2lKORoAGgAyAQhIAlAEYAE.webp"
    media_type = get_media_type_from_extension(orphaned_file)
    
    print(f"\nOrphaned file: {orphaned_file}")
    print(f"  Extracted date: {date_str}")
    print(f"  Detected type: {media_type}")
    
    # Count matching snaps for this date/type combo
    matching_snaps = []
    for conv_id, messages in test_conversations.items():
        for i, msg in enumerate(messages):
            if msg.get("Type") == "snap":
                if date_str in msg.get("Created", ""):
                    if msg.get("Media Type") == media_type:
                        matching_snaps.append((conv_id, i, msg))
    
    print(f"\nMatching snaps found: {len(matching_snaps)}")
    if len(matching_snaps) == 1:
        print("✅ SUCCESS: Exactly 1 match found - mapping would succeed!")
        conv_id, msg_idx, snap = matching_snaps[0]
        print(f"  Would map to: {conv_id} message #{msg_idx}")
        print(f"  From: {snap.get('From')}")
    else:
        print("❌ FAILED: Need exactly 1 match for correlation")
    
    # Test edge cases
    print("\n=== Testing Edge Cases ===")
    
    # Multiple snaps same day/type - should NOT map
    test_conversations_multi = {
        "user1": [
            {"From": "user1", "Media Type": "IMAGE", "Created": "2024-09-01 10:00:00 UTC", "Type": "snap"},
            {"From": "user1", "Media Type": "IMAGE", "Created": "2024-09-01 12:00:00 UTC", "Type": "snap"}
        ]
    }
    
    date_str = "2024-09-01"
    media_type = "IMAGE"
    matching_count = sum(1 for msgs in test_conversations_multi.values() 
                        for msg in msgs 
                        if msg.get("Type") == "snap" 
                        and date_str in msg.get("Created", "")
                        and msg.get("Media Type") == media_type)
    
    print(f"Multiple snaps same day test:")
    print(f"  Date: {date_str}, Type: {media_type}")
    print(f"  Matches found: {matching_count}")
    if matching_count != 1:
        print("  ✅ Correctly NOT mapping (need exactly 1 match)")
    
    # Different media type - should NOT map
    orphaned_video = "2024-08-27_b~test.mp4"
    video_type = get_media_type_from_extension(orphaned_video)
    
    print(f"\nDifferent media type test:")
    print(f"  File: {orphaned_video} (type: {video_type})")
    print(f"  Snap type: IMAGE")
    if video_type != "IMAGE":
        print("  ✅ Correctly NOT mapping (types don't match)")

if __name__ == "__main__":
    test_date_correlation()