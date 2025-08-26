# FFmpeg Overlay Merging Patterns

## Working Single File Pattern
The single file overlay merging is working despite using deprecated scale2ref.

## Deprecated Filter (Currently in use)
```
[1:v]format=rgba[ovr];[ovr][0:v]scale2ref[ovr2][base];[base][ovr2]overlay=0:0:format=auto[vout]
```

## Modern Replacement Filter
```
[1:v]scale=w='min(main_w,iw)':h='min(main_h,ih)',format=rgba[ovr];[0:v][ovr]overlay=(main_w-overlay_w)/2:(main_h-overlay_h)/2:format=auto[vout]
```

### Explanation:
1. `scale=w='min(main_w,iw)':h='min(main_h,ih)'` - Scales overlay to fit within video dimensions
2. `format=rgba` - Ensures overlay has alpha channel
3. `overlay=(main_w-overlay_w)/2:(main_h-overlay_h)/2` - Centers the overlay on the video
4. `format=auto` - Allows FFmpeg to choose optimal output format

## Alternative Simple Filter (tested working)
```
[1:v]scale=480:854[ovr];[0:v][ovr]overlay=0:0[vout]
```
This hardcodes dimensions but works reliably.

## Key Findings
- scale2ref is deprecated as of FFmpeg 7.x
- The warning "No filtered frames for output stream" indicates filter failure
- FFmpeg returns success (0) even when producing audio-only output
- Must validate output with ffprobe to ensure video stream exists