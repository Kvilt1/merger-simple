# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Snapchat media mapping tool that processes exported Snapchat data, organizes media files by conversation, and merges overlay/media pairs using FFmpeg.

## Common Development Tasks

### Running the Main Script
```bash
python map_media.py
```

### Dependencies
- Python 3.x with standard libraries (pathlib, json, re, shutil, struct, subprocess, logging, datetime, collections)
- FFmpeg (optional, for overlay merging functionality)
- Note: Missing `import os` in map_media.py (line 99 uses `os.utime` without import)

### Project Structure
- `map_media.py` - Main script that processes Snapchat exports
- `utils.py` - Helper functions for JSON handling and directory management
- `input/` - Place Snapchat export folders here (expected structure: `input/mydata/json/` and `input/mydata/chat_media/`)
- `output/` - Generated output with organized conversations and media

## Key Implementation Details

### Data Processing Flow
1. **Overlay Merging**: Merges media/overlay pairs using FFmpeg (if available)
2. **Data Loading**: Loads chat_history.json, snap_history.json, and friends.json
3. **Media Indexing**: Creates mapping of Media IDs to filenames
4. **Media Mapping**: Maps media to messages via:
   - Media ID matching
   - MP4 timestamp matching (for unmapped files)
5. **Output Organization**: Creates structured folders with conversation.json and media files

### Important Constants
- `TIMESTAMP_THRESHOLD_SECONDS = 10` - MP4 timestamp matching tolerance
- `QUICKTIME_EPOCH_ADJUSTER = 2082844800` - QuickTime to Unix epoch conversion

### Error Handling Notes
- The script continues processing even if FFmpeg is not installed (overlay merging is skipped)
- JSON loading failures return empty dictionaries rather than crashing
- Individual file processing errors are logged but don't halt the entire process

## Known Issues to Fix
- `map_media.py` uses `os.utime()` at line 99 but doesn't import `os` module
- Typo in `utils.py` line 38: `encoding='utf--8'` should be `encoding='utf-8'`