# Task Completion Checklist

## Before Marking a Task Complete

### 1. Code Quality Checks
Since no linting/formatting tools are currently configured, manually verify:
- [ ] Code follows snake_case naming conventions
- [ ] Type hints are present on all function signatures
- [ ] Docstrings are present for all functions
- [ ] No unused imports
- [ ] No debugging print statements left in code

### 2. Testing
Currently no test suite is configured. When implementing changes:
- [ ] Test the main entry point: `python main.py`
- [ ] Verify FFmpeg operations if media processing was modified
- [ ] Check that output directory structure is created correctly
- [ ] Ensure JSON files are valid and properly formatted
- [ ] Test with both default and custom arguments

### 3. Error Handling
- [ ] All file operations have appropriate try-except blocks
- [ ] Errors are logged with meaningful context
- [ ] Temporary resources are cleaned up on error
- [ ] Application handles missing FFmpeg gracefully

### 4. Git Hygiene
- [ ] Run `git status` to review changes
- [ ] Stage only intended changes
- [ ] Write clear, descriptive commit messages
- [ ] Ensure no temporary files are committed

### 5. Documentation
- [ ] Update docstrings if function behavior changed
- [ ] Update type hints if signatures changed
- [ ] Consider updating memory files if project structure changed significantly

## Recommended Post-Implementation Commands
```bash
# 1. Test the application
python main.py --log-level DEBUG

# 2. Review changes
git diff

# 3. Stage and commit if satisfied
git add -p  # Interactive staging
git commit -m "Descriptive message"
```

## Performance Considerations
- The application uses multiprocessing for FFmpeg operations
- Default uses (CPU count - 1) processes
- Monitor CPU usage during heavy media processing
- Consider memory usage with large exports