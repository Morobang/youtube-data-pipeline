# Section 2: Data Extraction Using the API

This section explains exactly what `video_stats.py` does, step by step.

---

## What Is an API?

An **API** (Application Programming Interface) is a way for two programs to talk to each other. YouTube has an API that lets you ask it questions like:

- "What videos does this channel have?"
- "How many views does this video have?"

You ask by sending an **HTTP request** to a URL (like visiting a website, but the response is structured data instead of a webpage). YouTube sends back a **JSON response** — a structured block of text that Python can read and work with.

To use the YouTube API you need an **API key** — a unique token that identifies you. Without it, YouTube won't respond. Your key lives in the `.env` file and is never written directly in the code.

---

## How the Script Works — The Big Picture

```
video_stats.py runs
        |
        v
1. get_playlist_id()    — asks YouTube: "what is the ID of this channel's uploads playlist?"
        |
        v
2. get_video_ids()      — asks YouTube: "give me every video ID in that playlist"
        |
        v
3. extract_video_data() — asks YouTube: "give me full details for all those video IDs"
        |
        v
4. save_to_json()       — writes all that data to a JSON file in ./data/
```

---

## Step 1 — `get_playlist_id()`

**The problem:** The YouTube API doesn't let you ask "give me all videos from channel X" directly. Instead, every channel has a hidden **uploads playlist** — a playlist that automatically contains every public video the channel has ever posted. To get the videos, you first need to find that playlist's ID.

**What it does:**

```python
url = (
    f"https://youtube.googleapis.com/youtube/v3/channels"
    f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
)
response = requests.get(url)
data = response.json()
playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
```

- It builds a URL using your `channel_handle` (e.g. `@mkbhd`) and `api_key`
- `requests.get(url)` sends the request and gets back a response
- `.json()` turns the raw text response into a Python dictionary
- It digs into the dictionary to find the playlist ID at the path `items[0] > contentDetails > relatedPlaylists > uploads`

**What a response looks like (simplified):**

```json
{
  "items": [
    {
      "contentDetails": {
        "relatedPlaylists": {
          "uploads": "UUBcRF18a7Qf58cCRy5xuWwQ"
        }
      }
    }
  ]
}
```

The playlist ID always starts with `UU`.

---

## Step 2 — `get_video_ids(playlist_id)`

**The problem:** A channel might have hundreds or thousands of videos. The YouTube API will only return a maximum of 50 at a time. To get all of them you have to ask repeatedly, moving to the next "page" each time.

**What it does:**

```python
page_token = None
base_url = (
    f"https://youtube.googleapis.com/youtube/v3/playlistItems"
    f"?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}"
)

while True:
    url = base_url
    if page_token:
        url += f"&pageToken={page_token}"

    response = requests.get(url)
    data = response.json()

    for item in data.get('items', []):
        video_ids.append(item['contentDetails'].get('videoId'))

    page_token = data.get('nextPageToken')
    if not page_token:
        break
```

- It starts with `page_token = None` (the first request has no token)
- After each response, it checks if the API sent back a `nextPageToken`
- If yes — there are more pages — it adds the token to the next request URL and loops again
- If no — all videos have been collected — the loop breaks
- It collects just the video IDs (e.g. `"dQw4w9WgXcQ"`) from each page

**Pagination visualised:**

```
Request 1 (no token)  →  returns videos 1–50   + nextPageToken: "abc123"
Request 2 (token=abc123) →  returns videos 51–100  + nextPageToken: "def456"
Request 3 (token=def456) →  returns videos 101–130 + no nextPageToken → stop
```

---

## Step 3 — `_batch(items, batch_size)`

This is a small helper function. The YouTube Videos API has a limit: you can only ask for details on **50 videos at a time**. If you have 300 video IDs you need to split them into 6 groups of 50 and make 6 separate requests.

```python
def _batch(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]
```

`yield` makes this a **generator** — instead of building the whole list at once and returning it, it produces one chunk at a time as you loop over it. This is memory-efficient.

**Example:**

```python
list(_batch([1, 2, 3, 4, 5], 2))
# → [[1, 2], [3, 4], [5]]
```

---

## Step 4 — `extract_video_data(video_ids)`

**What it does:** For each batch of up to 50 video IDs, it calls the YouTube Videos API to get full details, then flattens those details into a list of dictionaries.

```python
for batch in _batch(video_ids, max_results):
    ids = ",".join(batch)                   # "id1,id2,id3,..."
    url = (
        f"https://youtube.googleapis.com/youtube/v3/videos"
        f"?part=contentDetails&part=snippet&part=statistics&id={ids}&key={api_key}"
    )
    response = requests.get(url)
    data = response.json()

    for item in data.get('items', []):
        video_data = {
            "video_id": item['id'],
            "title": item['snippet'].get('title'),
            "published_at": item['snippet'].get('publishedAt'),
            "duration": item['contentDetails'].get('duration'),
            "view_count": item['statistics'].get('viewCount'),
            "like_count": item['statistics'].get('likeCount'),
            "comment_count": item['statistics'].get('commentCount'),
        }
        extracted_data.append(video_data)
```

**The three `part` parameters explained:**

The YouTube API lets you request different "parts" of a video's data. You only pay quota costs for what you request:

| Part | What's inside |
|---|---|
| `snippet` | Title, description, publish date, channel name, thumbnail URLs |
| `contentDetails` | Duration (in ISO 8601 format, e.g. `PT5M30S` = 5 minutes 30 seconds) |
| `statistics` | View count, like count, comment count |

**What one entry in the output list looks like:**

```json
{
  "video_id": "dQw4w9WgXcQ",
  "title": "Rick Astley - Never Gonna Give You Up",
  "published_at": "2009-10-25T06:57:33Z",
  "duration": "PT3M33S",
  "view_count": "1400000000",
  "like_count": "16000000",
  "comment_count": "2000000"
}
```

---

## Step 5 — `save_to_json(data)`

**What it does:** Writes the list of video dictionaries to a `.json` file in the `./data/` folder. The filename includes today's date so every run creates a new file instead of overwriting yesterday's.

```python
os.makedirs("./data", exist_ok=True)      # create ./data/ if it doesn't exist
filename = f"youtube_data_{date.today()}.json"
file_path = f"./data/{filename}"

with open(file_path, 'w', encoding='utf-8') as json_outfile:
    json.dump(data, json_outfile, indent=4, ensure_ascii=False)
```

- `os.makedirs(..., exist_ok=True)` — creates the folder; `exist_ok=True` means it won't crash if the folder already exists
- `json.dump(..., indent=4)` — writes the JSON with 4-space indentation so it's human-readable
- `ensure_ascii=False` — preserves non-English characters (e.g. Korean, Arabic video titles) instead of converting them to escape sequences

**Example output filename:** `./data/youtube_data_2026-06-20.json`

---

## Error Handling

Both API-calling functions wrap their code in `try/except` blocks:

```python
try:
    response = requests.get(url)
    response.raise_for_status()   # raises an exception for 4xx/5xx HTTP errors
    ...
except (KeyError, IndexError) as e:
    print(f"Unexpected API response structure: {e}")
    raise
except requests.exceptions.RequestException as e:
    print(f"HTTP request failed: {e}")
    raise
```

- `raise_for_status()` turns a silent HTTP 404 or 500 into a Python exception so the failure is obvious rather than hidden
- The `except` blocks print a helpful message and then re-raise the error so the program stops cleanly instead of continuing with bad data

---

## Running the Script

```bash
# Make sure your .env file has API_KEY and CHANNEL_HANDLE set, then:
python video_stats.py
```

The script will print progress to the terminal and save a JSON file to `./data/`.
