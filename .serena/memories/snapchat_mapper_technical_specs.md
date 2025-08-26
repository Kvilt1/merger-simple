# Snapchat Media Mapper - Technical Specifications

## Core Functionality:
Maps Snapchat exported media files to conversations using Media IDs and MP4 timestamps.

## Multi-part Video Detection Logic:
Triggers when:
1. Multiple overlays exist for the same date
2. All overlays are identical (MD5 hash comparison)
3. Media file count equals overlay count (excluding ~zip- files)

## FFmpeg Processing:
### Working Overlay Filter (FFmpeg 7.x):
```python
"[1:v][0:v]scale=w=rw:h=rh,format=rgba[ovr];[0:v][ovr]overlay=0:0:format=auto[vout]"
```

### Key Components:
- Input 0: Base video (MP4)
- Input 1: Overlay (WebP/PNG with alpha)
- Scale overlay to match video dimensions exactly
- Apply overlay at position 0:0

## File Exclusions:
- Thumbnails: Completely excluded from all processing
- media~zip- files: Stub files excluded from media counting
- Both filtered at copy operations (lines 473-474, 673-679)

## MP4 Timestamp Extraction:
- Reads creation time from QuickTime moov atom
- Adjusts from QuickTime epoch (1904) to Unix epoch
- Threshold: 10 seconds for matching messages

## Output Structure:
```
output/
├── conversations/  # Individual chats
├── groups/        # Group chats
│   └── media/
│       └── *_multipart/  # Multi-part video folders
│           ├── *.mp4     # Processed videos with overlays
│           └── timestamps.json
└── orphaned/      # Unmapped media (excluding thumbnails)
```

## Dependencies:
- FFmpeg (required for overlay merging)
- Python standard library only
- No external Python packages needed