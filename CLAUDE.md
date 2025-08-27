# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: Snapchat Media Mapper

A Python tool that processes Snapchat data exports to organize, merge, and map media files to conversations. The tool handles overlay merging with FFmpeg, intelligent media mapping, and creates a clean browsable archive.

## Critical Performance Optimization Context

The biggest bottleneck is **multipart and grouped folder processing** in `media_processing.py`. Recent optimizations include:
- mpire library for enhanced multiprocessing (with fallback to standard multiprocessing.Pool)
- VideoToolbox hardware acceleration support on macOS
- Optimized FFmpeg commands with presets and threading

### Known Issues with mpire
When using mpire WorkerPool, the worker function signature differs from standard multiprocessing:
- mpire may unpack tuples differently when using `pool.map()`
- Worker functions should handle both tuple and unpacked argument patterns for compatibility

## Development Commands

### Running the Application
```bash
# Main execution (from project root)
python src/main.py

# With custom directories
python src/main.py --input /path/to/input --output /path/to/output

# Debug mode with detailed logging
python src/main.py --log-level DEBUG

# Without cleaning output directory
python src/main.py --no-clean
```

### Installing Dependencies
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install requirements
pip install -r requirements.txt

# Optional: Install optimization library
pip install mpire  # For enhanced multiprocessing performance
```

### Code Quality (Not Currently Configured)
```bash
# Consider adding these tools:
pip install ruff black mypy
black src/*.py  # Format code
ruff check src/*.py  # Lint
mypy src/*.py  # Type check
```

## High-Level Architecture

### Module Structure
The codebase is organized into 4 core modules in `src/`:

1. **config.py**: Central configuration, utilities, and logging setup
   - Constants: INPUT_DIR, OUTPUT_DIR, TIMESTAMP_THRESHOLD_SECONDS
   - Utilities: JSON loading/saving, timestamp formatting, path operations

2. **media_processing.py**: Core media handling and optimization
   - FFmpeg overlay merging with hardware acceleration detection
   - Multipart video processing (handles videos split into segments)
   - Grouped media processing (multiple files with identical overlays)
   - Media ID extraction and timestamp-based mapping
   - Performance optimizations with mpire/multiprocessing pools

3. **conversation.py**: Conversation data processing
   - Merges chat_history.json and snap_history.json
   - Enriches with friends.json data (display names, status)
   - Creates conversation metadata and structures

4. **main.py**: Orchestration and CLI interface
   - Command-line argument parsing
   - Workflow coordination between modules
   - Media copying and orphaned media handling

### Processing Flow
1. **Discovery Phase**: Locate Snapchat export in input/ directory
2. **Media Pairing**: Match media files with overlay files by date/timestamp
3. **Parallel Merging**: Use multiprocessing pool to merge overlays with FFmpeg
4. **Multipart Processing**: Detect and process segmented videos (e.g., part1.mp4, part2.mp4)
5. **Grouped Processing**: Handle media with identical overlays efficiently
6. **Conversation Mapping**: Map processed media to conversations using media IDs and timestamps
7. **Output Generation**: Create organized folder structure with conversation.json files

### Key Optimization Points
- **Parallel Processing**: Uses process pools (mpire preferred, fallback to multiprocessing)
- **Hardware Acceleration**: Detects and uses VideoToolbox on macOS for faster FFmpeg operations
- **Batch Operations**: Groups similar operations to reduce overhead
- **Hash-based Deduplication**: Uses SHA256 for identifying duplicate overlays

## FFmpeg Requirements
FFmpeg is required for media merging operations. The project detects hardware acceleration:
```bash
# Check FFmpeg installation
which ffmpeg
ffmpeg -version

# Check for VideoToolbox support (macOS)
ffmpeg -hide_banner -hwaccels | grep videotoolbox
```

## Directory Structure Expectations
```
merger-simple/
├── input/           # Place Snapchat export here
│   └── mydata/     # Unzipped export folder
│       ├── json/   # Contains chat_history.json, friends.json, snap_history.json
│       └── chat_media/  # All media files
├── output/          # Generated organized output
│   ├── conversations/   # Individual chat folders
│   ├── groups/         # Group chat folders
│   └── orphaned/       # Unmapped media
└── src/            # Source code
```

## Performance Considerations
- Large exports benefit from SSD storage for I/O operations
- Default uses min(8, CPU_count - 2) workers for parallel processing
- Hardware acceleration provides 2-5x speedup for video processing
- Memory usage scales with number of media files (indexing phase)

## Type Hints and Code Style
- Comprehensive type hints using typing module (Dict, List, Optional, Set, Tuple, Any)
- Snake_case naming convention for functions and variables
- UPPER_SNAKE_CASE for constants
- Pathlib.Path used consistently for all path operations
- Logging throughout with appropriate levels (DEBUG, INFO, WARNING, ERROR)

## Testing Approach
While no formal test suite exists, testing scripts are available:
- `test_multipart_performance.py`: Benchmarks multipart processing
- `test_date_correlation.py`: Validates date matching logic

Run performance tests:
```bash
python test_multipart_performance.py
```

## Common Development Tasks

### Debugging Media Processing Issues
```bash
# Enable debug logging to see detailed processing steps
python src/main.py --log-level DEBUG

# Check for FFmpeg errors in logs
grep -i "ffmpeg" output.log
grep -i "error" output.log
```

### Monitoring Performance
```bash
# Use time command to measure execution
time python src/main.py

# Monitor CPU usage during processing
# In another terminal:
htop  # or Activity Monitor on macOS
```

### Working with Git
```bash
# Feature branch workflow
git checkout -b feature/optimization-name
git add -p  # Stage changes interactively
git diff --staged  # Review before commit
git commit -m "Optimize: specific improvement description"
```