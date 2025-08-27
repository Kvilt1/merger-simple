# Project Structure

## Directory Layout
```
merger-simple/
├── src/                    # Source code directory
│   ├── config.py          # Configuration, utilities, logging
│   ├── conversation.py    # Conversation processing logic
│   ├── media_processing.py # Media merging and mapping
│   └── main.py            # Main orchestration and CLI
├── input/                 # Default input directory for Snapchat exports
├── output/                # Default output directory for processed data
└── .serena/              # Serena project configuration

```

## Module Responsibilities

### config.py
- Global configuration constants (INPUT_DIR, OUTPUT_DIR, thresholds)
- Logging setup and logger instance
- Utility functions: ensure_directory, load_json, save_json, format_timestamp, sanitize_filename

### media_processing.py
- FFmpeg integration for merging media with overlays
- Parallel processing with multiprocessing.Pool
- Media file indexing and ID extraction
- Timestamp-based media mapping
- Multi-part and grouped media handling

### conversation.py
- Conversation data merging and processing
- Friends data processing
- Account owner determination
- Conversation metadata generation
- Participant extraction and folder naming

### main.py
- Command-line interface with argparse
- Main orchestration flow
- Export folder discovery
- File copying for unmerged media
- Orphaned media handling

## Data Flow
1. Find Snapchat export folder in input directory
2. Process and merge overlay/media pairs using FFmpeg
3. Index all media files and extract IDs
4. Load and merge conversation JSONs
5. Process friends data
6. Map media to messages using IDs and timestamps
7. Organize into conversation-based folder structure
8. Handle orphaned media separately

## Key Data Structures
- Media index: Dict mapping media IDs to filenames
- Conversations: Dict of conversation ID to message lists
- Mappings: Nested dict structure for media-to-message associations
- Merged files: Set tracking successfully merged media/overlay pairs