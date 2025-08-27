"""Optimization utilities for handling large datasets."""

import os
import psutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import lru_cache
from typing import Optional

@lru_cache(maxsize=1)
def get_optimal_worker_count(task_type: str = "cpu") -> int:
    """Determine optimal number of workers based on system resources and task type."""
    cpu_count = os.cpu_count() or 1
    memory_gb = psutil.virtual_memory().total / (1024**3)
    
    if task_type == "ffmpeg":
        # FFmpeg is CPU-intensive and memory-heavy
        # Leave some cores for system and other processes
        optimal = min(cpu_count - 1, int(memory_gb / 2))
        return max(2, optimal)
    elif task_type == "io":
        # I/O operations benefit from more threads
        return min(cpu_count * 2, 32)
    else:
        # General CPU tasks
        return max(2, cpu_count - 1)

def get_memory_limit() -> int:
    """Get safe memory limit for processing (75% of available)."""
    available_mb = psutil.virtual_memory().available / (1024**2)
    return int(available_mb * 0.75)

def should_use_parallel(file_count: int, total_size_mb: float) -> bool:
    """Determine if parallel processing would be beneficial."""
    if file_count < 10:
        return False
    if total_size_mb < 100:
        return False
    return True

def create_executor(task_type: str = "cpu") -> Optional[ThreadPoolExecutor]:
    """Create appropriate executor for task type."""
    workers = get_optimal_worker_count(task_type)
    
    if task_type == "io":
        return ThreadPoolExecutor(max_workers=workers)
    else:
        return ProcessPoolExecutor(max_workers=workers)