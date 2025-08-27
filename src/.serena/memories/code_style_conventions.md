# Code Style and Conventions

## Python Style Guidelines
- **PEP 8 Compliance**: Generally follows PEP 8 standards
- **Naming Conventions**:
  - Functions: snake_case (e.g., `merge_overlay_pairs`, `extract_media_id`)
  - Constants: UPPER_SNAKE_CASE (e.g., `INPUT_DIR`, `TIMESTAMP_THRESHOLD_SECONDS`)
  - Variables: snake_case (e.g., `media_files`, `overlay_groups`)
  - Private functions: Leading underscore (e.g., `_ffmpeg_worker`)

## Type Hints
- Comprehensive type hints on all function signatures
- Using typing module imports: Dict, List, Optional, Set, Tuple, Any
- Return type annotations for all functions
- Example: `def load_json(path: Path) -> Dict[str, Any]:`

## Documentation
- Module-level docstrings at the top of each file
- Function docstrings using triple quotes
- Brief, descriptive docstrings explaining function purpose
- No excessive inline comments

## Code Organization
- Imports grouped by: standard library, third-party, local modules
- Constants defined at module level
- Logger instances created per module
- Functions organized logically by purpose

## Error Handling
- Try-except blocks for I/O operations and external process calls
- Logging errors with context using logger.error()
- Graceful fallbacks (e.g., returning empty dict/None on error)
- Cleanup of temporary resources in finally blocks or error handlers

## Path Handling
- Using pathlib.Path consistently throughout
- No string concatenation for paths
- Path.exists() checks before operations
- Path.mkdir(parents=True, exist_ok=True) for directory creation