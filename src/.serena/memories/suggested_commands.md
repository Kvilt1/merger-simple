# Suggested Commands for Development

## Running the Application
```bash
# Main entry point with default directories
python main.py

# With custom input/output directories
python main.py --input /path/to/input --output /path/to/output

# Without cleaning output directory
python main.py --no-clean

# With custom log level
python main.py --log-level DEBUG
```

## Python Development Commands
```bash
# Run the main script
python3 main.py

# Run with module syntax
python -m main

# Check Python version
python --version
```

## Code Quality Tools
Note: No linting or formatting tools are currently configured in the project.
Consider adding:
```bash
# Install development tools
pip install ruff black mypy

# Format code with black
black *.py

# Lint with ruff
ruff check *.py

# Type check with mypy
mypy *.py
```

## Git Commands (Darwin/macOS)
```bash
# Check status
git status

# Stage changes
git add .

# Commit changes
git commit -m "message"

# View commit history
git log --oneline -10

# Create feature branch
git checkout -b feature/branch-name
```

## File System Commands (Darwin/macOS)
```bash
# List files with details
ls -la

# Navigate directories
cd src/

# Find files
find . -name "*.py"

# Search in files (using ripgrep if available)
rg "pattern" --type py

# View file contents
cat filename.py

# Create directory
mkdir -p output/temp
```

## FFmpeg Check
```bash
# Check if FFmpeg is installed (required for media merging)
which ffmpeg

# Check FFmpeg version
ffmpeg -version
```

## System Information
```bash
# Check system
uname -s  # Should show "Darwin" on macOS

# Check available CPU cores (used for multiprocessing)
sysctl -n hw.ncpu
```