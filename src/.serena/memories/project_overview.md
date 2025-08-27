# Snapchat Media Mapper Project Overview

## Purpose
This project processes Snapchat data exports to organize and map media files to conversations. It specifically:
- Merges overlay files with media files using FFmpeg
- Maps media files to conversation messages using media IDs and timestamps
- Organizes exported data into a structured output format with conversation folders
- Handles multi-part videos and grouped media with identical overlays
- Processes orphaned media that couldn't be mapped to conversations

## Tech Stack
- **Language**: Python 3.x
- **Type Hints**: Comprehensive type hints using typing module (Dict, List, Optional, Set, Tuple, Any)
- **External Dependencies**: FFmpeg (required for media merging)
- **Standard Libraries**: json, logging, pathlib, datetime, multiprocessing, subprocess, hashlib, struct, re, shutil

## Architecture
- **Modular Design**: Separated into 4 main modules:
  - `config.py`: Configuration, utilities, and logging setup
  - `media_processing.py`: Media file handling, merging, and mapping
  - `conversation.py`: Conversation data processing and metadata generation
  - `main.py`: Main orchestration and CLI interface
- **Multiprocessing**: Uses process pools for parallel FFmpeg operations
- **Logging**: Comprehensive logging throughout the application

## Key Features
- Parallel processing of media merging operations using multiprocessing.Pool
- Intelligent pairing of media and overlay files by date and timestamp
- Support for multi-part videos with identical overlays
- Grouped folder creation for related media files
- Media mapping using both media IDs and timestamp proximity matching
- Automatic cleanup and organization of output directory structure