#!/usr/bin/env python3
"""Bitmoji processing: extraction, fallback generation, and avatar pool management."""

import colorsys
import hashlib
import logging
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Semaphore
from typing import Dict, Optional, Set, Tuple

import requests

logger = logging.getLogger(__name__)

# === Configuration ===
TARGET_SIZE = 54  # SVG viewBox size for avatars
REQUEST_SIZE = 512  # Size to request from Snapchat API
MAX_WORKERS = 50  # Maximum concurrent requests
MAX_REQUESTS_PER_SECOND = 50  # Rate limiting
ENABLE_BITMOJI = True  # Enable Bitmoji in snapcode

# === XML Namespaces ===
SVG_NS = "http://www.w3.org/2000/svg"
XLINK_NS = "http://www.w3.org/1999/xlink"
ET.register_namespace('', SVG_NS)
ET.register_namespace('xlink', XLINK_NS)

# === Fallback avatar generation ===
ASSIGNED_COLORS = {}
ASSIGNED_HUES = set()
ASSIGN_LOCK = threading.Lock()

# Default avatar path for users without Bitmoji
UNMAPPED_SVG_PATH_D = (
    "M27 54.06C33.48 54.06 39.48 51.78 44.16 47.94C43.32 46.68 42.36 45.78 41.34 44.94C38.22 42.48 33.78 41.58 30.72 41.04L30.6 39.84C35.28 37.08 36.42 34.14 38.28 27.96L38.34 27.54C38.34 27.54 39.96 26.88 40.2 23.88C40.56 19.8 38.88 21 38.88 20.7C39.06 18.6 39 15.84 38.4 13.8C37.14 9.42 32.88 5.94 27 5.94C21.12 5.94 16.86 9.36 15.6 13.8C15 15.84 14.94 18.6 15.12 20.76C15.12 21.06 13.5 19.86 13.8 23.94C14.04 26.94 15.66 27.6 15.66 27.6L15.72 28.02C17.58 34.2 18.72 37.14 23.4 39.9L23.28 41.1C20.28 41.64 15.78 42.54 12.66 45C11.64 45.84 10.68 46.74 9.84 48C14.52 51.78 20.52 54.06 27 54.06Z"
)


class RateLimiter:
    """Rate limiting mechanism for API requests."""
    
    def __init__(self, max_per_second):
        self.max_per_second = max_per_second
        self.semaphore = Semaphore(max_per_second)
        self.reset_time = time.time()
    
    def __enter__(self):
        self.semaphore.acquire()
        current_time = time.time()
        if current_time - self.reset_time >= 1.0:
            self.reset_time = current_time
            for _ in range(self.max_per_second - 1):
                try:
                    self.semaphore.release()
                except ValueError:
                    pass
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        time.sleep(1.0 / self.max_per_second)
        self.semaphore.release()


rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)


def _hsl_to_hex(h: float, s: float, l: float) -> str:
    """Convert HSL color to hex string."""
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return "#{:02x}{:02x}{:02x}".format(
        int(round(r * 255)),
        int(round(g * 255)),
        int(round(b * 255))
    )


def _hash_to_hue(name: str) -> float:
    """Generate a hue value from username hash."""
    return float(int(hashlib.sha256(name.encode("utf-8")).hexdigest(), 16) % 360)


def _distinct_color_for_username(name: str, sat: float = 0.30, light: float = 0.60, 
                                min_sep: float = 12.0) -> str:
    """Generate a distinct color for a username, avoiding similar colors."""
    with ASSIGN_LOCK:
        if name in ASSIGNED_COLORS:
            return ASSIGNED_COLORS[name]
        
        base = _hash_to_hue(name)
        hue = base
        golden = 137.508
        tries = 0
        
        while any(min((abs(hue - h), 360.0 - abs(hue - h))) < min_sep 
                 for h in ASSIGNED_HUES) and tries < 720:
            hue = (base + golden * (tries + 1)) % 360.0
            tries += 1
        
        ASSIGNED_HUES.add(hue)
        hexcol = _hsl_to_hex(hue / 360.0, sat, light)
        ASSIGNED_COLORS[name] = hexcol
        return hexcol


def build_fallback_avatar_svg(username: str, target_size: int = TARGET_SIZE) -> str:
    """Build a fallback avatar SVG with distinct color for users without Bitmoji."""
    fill_hex = _distinct_color_for_username(username)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<svg viewBox="0 0 {target_size} {target_size}" version="1.1" '
        f'preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">'
        f'<path d="{UNMAPPED_SVG_PATH_D}" fill="{fill_hex}" '
        f'stroke="black" stroke-opacity="0.2" stroke-width="0.9"/>'
        f'</svg>'
    )


def get_bitmoji_data(username: str, size: Optional[int] = REQUEST_SIZE, 
                    bitmoji: bool = ENABLE_BITMOJI) -> str:
    """Fetch Snapcode SVG data for a Snapchat username."""
    with rate_limiter:
        base_url = "https://app.snapchat.com/web/deeplink/snapcode"
        params = {
            "username": username,
            "type": "SVG"
        }
        if size:
            params["size"] = size
        if bitmoji:
            params["bitmoji"] = "enable"
        
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.text


def _find_image_element(svg_root):
    """Find the embedded Bitmoji <image> element inside the Snapcode SVG."""
    image = svg_root.find(f".//{{{SVG_NS}}}image")
    if image is None:
        image = svg_root.find(".//image")
    return image


def _get_href(image_element):
    """Return the data URL/href from the image element."""
    href = image_element.get(f"{{{XLINK_NS}}}href") or image_element.get("href")
    if not href:
        # Some files use 'xlink:href' attribute name literally
        href = image_element.attrib.get("xlink:href")
    return href


def extract_bitmoji_svg(svg_content: str, username: str, 
                        target_size: int = TARGET_SIZE) -> str:
    """
    Extract Bitmoji from Snapcode SVG or generate fallback avatar.
    Returns a normalized 54x54 SVG suitable for React components.
    """
    try:
        root = ET.fromstring(svg_content)
    except ET.ParseError:
        logger.warning(f"Could not parse SVG for {username}, using fallback")
        return build_fallback_avatar_svg(username, target_size)
    
    image_element = _find_image_element(root)
    if image_element is None:
        # No Bitmoji found, generate fallback
        logger.info(f"No Bitmoji found for {username}, using fallback avatar")
        return build_fallback_avatar_svg(username, target_size)
    
    href = _get_href(image_element)
    if not href:
        logger.warning(f"No href found for {username}, using fallback")
        return build_fallback_avatar_svg(username, target_size)
    
    # Build new minimal SVG root
    new_root = ET.Element(f"{{{SVG_NS}}}svg", {
        "viewBox": f"0 0 {target_size} {target_size}",
        "version": "1.1",
        "preserveAspectRatio": "xMidYMid meet",
    })
    
    # Create sanitized <image> that fills the viewport
    new_image = ET.Element(f"{{{SVG_NS}}}image", {
        "x": "0",
        "y": "0",
        "width": str(target_size),
        "height": str(target_size),
        "preserveAspectRatio": "xMidYMid meet",
    })
    
    # Set href for broad renderer support
    new_image.set(f"{{{XLINK_NS}}}href", href)
    new_image.set("href", href)  # SVG2-compatible
    
    # Remove any transforms/clipping
    for attr in ("transform", "clip-path", "style"):
        if attr in new_image.attrib:
            del new_image.attrib[attr]
    
    new_root.append(new_image)
    
    # Serialize
    svg_string = ET.tostring(new_root, encoding='unicode', method='xml')
    if not svg_string.startswith('<?xml'):
        svg_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + svg_string
    
    return svg_string


def fetch_bitmoji_for_user(username: str) -> Tuple[str, Optional[str], str]:
    """
    Fetch Bitmoji for a single user.
    Returns (username, svg_content or None, status).
    Status can be: 'success', 'fallback', 'error'
    """
    if not username:
        return username, None, 'error'
    
    try:
        # Fetch snapcode data
        svg_content = get_bitmoji_data(username)
        
        # Extract and normalize Bitmoji
        bitmoji_svg = extract_bitmoji_svg(svg_content, username)
        
        # Check if it's a fallback (contains our default path)
        if UNMAPPED_SVG_PATH_D in bitmoji_svg:
            return username, bitmoji_svg, 'fallback'
        
        return username, bitmoji_svg, 'success'
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.info(f"User {username} not found (404), using fallback")
            fallback_svg = build_fallback_avatar_svg(username)
            return username, fallback_svg, 'fallback'
        else:
            logger.error(f"HTTP error for {username}: {e}")
            fallback_svg = build_fallback_avatar_svg(username)
            return username, fallback_svg, 'fallback'
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error for {username}: {e}")
        fallback_svg = build_fallback_avatar_svg(username)
        return username, fallback_svg, 'fallback'
    
    except Exception as e:
        logger.error(f"Unexpected error for {username}: {e}")
        fallback_svg = build_fallback_avatar_svg(username)
        return username, fallback_svg, 'fallback'


def extract_bitmojis(usernames: Set[str], max_workers: int = MAX_WORKERS) -> Dict[str, str]:
    """
    Extract Bitmojis for multiple usernames concurrently.
    Returns dict of username -> SVG content.
    """
    logger.info(f"Extracting Bitmojis for {len(usernames)} users")
    
    results = {}
    stats = {'success': 0, 'fallback': 0, 'error': 0}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_username = {
            executor.submit(fetch_bitmoji_for_user, username): username
            for username in usernames
        }
        
        for future in as_completed(future_to_username):
            username, svg_content, status = future.result()
            
            if svg_content:
                results[username] = svg_content
                stats[status] = stats.get(status, 0) + 1
                
                if status == 'success':
                    logger.debug(f"✓ {username}: Bitmoji extracted")
                elif status == 'fallback':
                    logger.debug(f"○ {username}: Using fallback avatar")
            else:
                stats['error'] = stats.get('error', 0) + 1
                logger.warning(f"✗ {username}: Failed to extract")
    
    logger.info(f"Bitmoji extraction complete: {stats['success']} success, "
                f"{stats['fallback']} fallback, {stats['error']} errors")
    
    return results


def hash_svg_content(svg_content: str) -> str:
    """Generate SHA-1 hash of SVG content for filename."""
    return hashlib.sha1(svg_content.encode('utf-8')).hexdigest()


def save_avatar_pool(avatars: Dict[str, str], avatar_dir: Path) -> Dict[str, str]:
    """
    Save avatars to pool directory with content-based hashing.
    Returns dict of username -> avatar path.
    """
    avatar_dir.mkdir(parents=True, exist_ok=True)
    username_to_path = {}
    
    for username, svg_content in avatars.items():
        # Generate content-based filename
        file_hash = hash_svg_content(svg_content)
        filename = f"{file_hash}.svg"
        filepath = avatar_dir / filename
        
        # Write file if it doesn't exist (deduplication)
        if not filepath.exists():
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(svg_content)
        
        # Store web path
        username_to_path[username] = f"/a/{filename}"
    
    return username_to_path