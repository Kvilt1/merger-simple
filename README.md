# Snapchat Export Merger & Organizer

A Python tool that processes and organizes your exported Snapchat data. It merges chat and snap histories, links them with your friends' information, and intelligently maps associated media files to each conversation — creating a clean, browsable archive of your data.

The script organizes all chats into individual folders, each containing a detailed JSON file of the conversation and a `media/` subfolder with all corresponding images and videos.

---

## Features

- **Chronological conversations:** Merge `chat_history.json` and `snap_history.json` into a single, chronologically sorted timeline for each conversation.
- **Media organization:** Copy and organize all media from the `chat_media/` folder into conversation-specific subdirectories.
- **Intelligent media mapping:** Map media files to their corresponding messages using unique media IDs, with a timestamp-matching fallback for unmapped files.
- **Friend data integration:** Enrich conversation data with information from `friends.json` (display names, friend status including deleted, how they were added).
- **Orphaned media handling:** Isolate any media files that cannot be mapped to a specific conversation into an `orphaned/` folder for manual review.
- **Structured output:** Generate a clean, well-organized output directory with separate folders for individual chats and group chats. Each folder contains a detailed `conversation.json` with metadata and the full message history.

---

## Requirements

- **Python 3.x** (standard library only)
- **Snapchat Data Export:** Your unzipped Snapchat export folder (e.g., `mydata`).

---

## How to Use

1. **Place your data**  
   Put your unzipped Snapchat export folder (e.g., `mydata`) inside the `input/` directory. The script will automatically locate it. The expected structure is:

   ```text
   .
   ├── input/
   │   └── mydata/
   │       ├── json/
   │       │   ├── chat_history.json
   │       │   ├── friends.json
   │       │   └── snap_history.json
   │       └── chat_media/
   │           └── ... (all your media files)
   └── src/
       └── main.py
   ```

2. **Run the script** from the root of the repository:

   ```bash
   python src/main.py
   ```

---

## Command-Line Arguments

Customize behavior with the following flags:

- `--input <path>`: Custom input directory. **Defaults to** `./input`.
- `--output <path>`: Custom output directory. **Defaults to** `./output`.
- `--no-clean`: Do not delete contents of the output directory before running.
- `--log-level <level>`: Set logging level (`DEBUG`, `INFO`, `WARNING`). **Defaults to** `INFO`.

**Example:**

```bash
python src/main.py --output ./my_snap_archive --no-clean
```

---

## Output Structure

After running, the `output/` directory will be organized like this:

```text
output/
├── conversations/
│   └── 2025-08-18 - Maiken Jensen/
│       ├── conversation.json
│       └── media/
│           ├── image1.jpg
│           └── video1.mp4
├── groups/
│   └── 2025-08-18 - Einkigin/
│       ├── conversation.json
│       └── media/
│           └── ...
└── orphaned/
    └── unmapped_media.jpg
```

**What’s inside**

- **`conversations/` & `groups/`:** Folders for each individual and group chat, named using the last message date and the participant’s or group’s name.
- **`conversation.json`:** For each chat folder, this file contains:
  - **Metadata:** Participants, message counts, date ranges, etc.
  - **Messages:** Complete, chronologically sorted list of all messages and snaps.
- **`media/`:** All media files that were successfully mapped to that conversation.
- **`orphaned/`:** Any media files that could not be mapped to a conversation.

---

## How It Works

1. **Data loading:** Reads `chat_history.json`, `snap_history.json`, and `friends.json` from the export’s `json/` directory.
2. **Conversation merging:** Combines chat and snap histories into a unified list per conversation, sorted by microsecond timestamp.
3. **Friend processing:** Builds a quick-lookup map of all friends (including deleted) to enrich conversations with display names and status.
4. **Media indexing & mapping:**
   - Indexes every file in your `chat_media/` directory.
   - Iterates through every message in every conversation, mapping media files to messages.
   - Uses unique media IDs when available and falls back to comparing message timestamps with file timestamps when necessary.
5. **Output generation:** Creates the structured folder system, copies mapped media into the correct directories, and saves `conversation.json` for each chat.

---

## Notes

- Your original Snapchat export is **never modified**.
- If you re-run the script without `--no-clean`, the `output/` directory will be cleared first.
- Large exports can take some time due to file I/O; using a fast disk helps.

---

## Disclaimer

This project is not affiliated with Snapchat. Use it only with data you own and respect the privacy of others.
