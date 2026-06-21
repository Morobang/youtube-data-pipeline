# Section 2: Data Extraction Using the API

## What we're doing in this section

The first step of the pipeline is getting data out of YouTube. That's what `video_stats.py` does. By the end of this section you'll understand every single line in that file — not just *what* it does, but *why* it does it that way.

Let's start from the very beginning.

---

## What is an API and why do we need one?

You might think: "why can't I just go to a YouTube channel page, copy the data, and save it?" The short answer is — you can, once. But you can't automate it, because YouTube's website is designed for humans in browsers, not for Python scripts. The layout changes, things load dynamically with JavaScript, and YouTube actively blocks programs that try to scrape it.

An **API** (Application Programming Interface) is YouTube's official, supported way for programs to request data. Instead of a webpage, you get clean, structured data in a format your code can easily work with.

Here's the mental model: think of the API as a restaurant menu. You don't walk into the kitchen and grab food yourself — you order from the menu, the kitchen prepares it, and it comes back in a predictable format. The API is the menu. Your code is the customer placing the order.

---

## How an API request actually works

When your Python script calls the YouTube API, this is what happens:

1. Your script builds a URL — a web address with your question embedded in it
2. The `requests` library sends that URL as an HTTP request (the same kind your browser sends when you visit a website)
3. YouTube's servers receive it, check your API key, fetch the data, and send back a response
4. The response comes back as **JSON** — a structured text format that looks like a Python dictionary

The whole thing happens in milliseconds. From your code's perspective it's just: send a URL, get back a dictionary.

---

## The API key — and why it lives in `.env`

Every request you send to YouTube must include your **API key** — a unique string that proves you're an authorised user. YouTube uses this to track usage and enforce limits (you get a certain number of requests per day for free).

Here's where beginners often make a mistake: they write the key directly in the code.

```python
# DON'T DO THIS
api_key = "AIzaSyD_abc123yourrealkey"
```

If you push that to GitHub, your key is now public. Anyone can find it, use it, and burn through your daily quota. That's why your `.env` file exists:

```bash
# .env  (never committed to git)
API_KEY=AIzaSyD_abc123yourrealkey
```

And in your code:

```python
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("API_KEY")
```

`load_dotenv()` reads the `.env` file and puts each variable into the environment. `os.getenv("API_KEY")` reads it back. Your key never appears in any file that gets committed.

---

## The flow of `video_stats.py`

The script runs four functions in sequence. Each one feeds its result into the next:

```
get_playlist_id()
      ↓ returns a playlist ID
get_video_ids(playlist_id)
      ↓ returns a list of video IDs
extract_video_data(video_ids)
      ↓ returns a list of video dictionaries
save_to_json(video_data)
      ↓ saves to ./data/youtube_data_2026-06-21.json
```

Let's go through each one.

---

## `get_playlist_id()` — finding where the videos live

You might expect the YouTube API to let you say "give me all videos from channel X." But it doesn't work that way. Internally, every YouTube channel has a hidden **uploads playlist** — a playlist that automatically contains every public video the channel has ever posted. The API makes you go through the playlist to get to the videos.

So the first step is to find that playlist's ID.

```python
url = (
    f"https://youtube.googleapis.com/youtube/v3/channels"
    f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
)
response = requests.get(url)
data = response.json()
playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
```

Break it down piece by piece:

**The URL** is a question written in a format the API understands. The `?` starts the query parameters. `part=contentDetails` says "I want the contentDetails section of the response". `forHandle=@channelname` says which channel. `key=` is your credentials.

**`requests.get(url)`** sends the request. Think of it as your code opening a browser tab, going to that URL, and getting the response — except it happens in code, not a browser.

**`.json()`** converts the raw text response into a Python dictionary. Without this you'd just have a long string of characters you can't navigate.

**The deep access** — `data['items'][0]['contentDetails']['relatedPlaylists']['uploads']` — is navigating the nested dictionary. The API response is a JSON object with a specific structure, and you're drilling down to find the uploads playlist ID inside it.

What does the response actually look like? Simplified, it's this:

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

The playlist ID always starts with `UU`. That's what gets returned and passed to the next function.

---

## `get_video_ids(playlist_id)` — the pagination problem

Now that you have the playlist ID, you want all the video IDs in it. But here's the catch: the YouTube API will only give you **50 videos at a time**. If a channel has 300 videos, you need to make 6 separate requests and stitch the results together.

The API handles this with **pagination**. After each response, if there are more results, the API includes a `nextPageToken` in the response. To get the next page, you include that token in your next request. When there's no more token, you're done.

Here's how the code handles this:

```python
video_ids = []
page_token = None

while True:
    url = base_url
    if page_token:
        url += f"&pageToken={page_token}"

    response = requests.get(url)
    data = response.json()

    for item in data.get('items', []):
        video_id = item['contentDetails'].get('videoId')
        if video_id:
            video_ids.append(video_id)

    page_token = data.get('nextPageToken')
    if not page_token:
        break
```

Walk through the logic:
- Start with `page_token = None` — the first request doesn't have one
- Enter an infinite `while True` loop — we don't know how many pages there are upfront
- Build the URL, and if we have a token from a previous response, attach it
- After getting the response, collect all video IDs from this page
- Check if the response has a `nextPageToken`
- If yes — there's another page — save the token and loop again
- If no — we've got everything — break out of the loop

Visualised:

```
Request 1 (no token)     → videos 1–50   + nextPageToken: "CAoQAA"
Request 2 (CAoQAA)       → videos 51–100 + nextPageToken: "CAoQAB"
Request 3 (CAoQAB)       → videos 101–130 + no nextPageToken → stop
```

At the end, `video_ids` is a flat list of every video ID on the channel — could be 50, could be 500.

---

## `_batch()` — why we need to chunk the IDs

Now you have all the video IDs but you need the actual stats for each video. The YouTube Videos API can look up multiple IDs in one request — but only up to **50 at a time**. So again, you need to split your list into chunks.

That's all `_batch()` does:

```python
def _batch(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]
```

The `yield` keyword makes this a **generator**. Instead of building the whole list of chunks and returning it at once, it hands you one chunk at a time as you loop over it. This is a small memory optimisation — you never have all chunks sitting in memory simultaneously.

Here's the output if you were to collect it into a list:

```python
list(_batch(['a', 'b', 'c', 'd', 'e'], 2))
# → [['a', 'b'], ['c', 'd'], ['e']]
```

The last chunk is smaller if the list doesn't divide evenly — that's fine, the API handles it.

---

## `extract_video_data(video_ids)` — getting the actual stats

This is where you get the data you actually care about: titles, view counts, durations, like counts. For each batch of up to 50 video IDs, you make one API call that returns full details for all of them.

```python
for batch in _batch(video_ids, max_results):
    ids = ",".join(batch)
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

A few things worth understanding here:

**`",".join(batch)`** turns a list like `['abc', 'def', 'xyz']` into the string `"abc,def,xyz"`. The API accepts multiple IDs as a comma-separated list in the URL.

**The `part` parameters** tell the API which sections of data to include. The YouTube Videos API splits video data into sections, and you only get (and pay quota for) what you ask for:

- `part=snippet` — gives you title, description, publish date, channel name
- `part=contentDetails` — gives you duration (as `"PT5M30S"` — you'll convert this in Silver)
- `part=statistics` — gives you view count, like count, comment count

**`.get('viewCount')`** is used instead of `['viewCount']` because statistics can sometimes be missing — for example, a channel owner can disable like counts. `.get()` returns `None` instead of crashing.

The result after looping through all batches is a list of dictionaries — one per video — that looks like this:

```json
[
  {
    "video_id": "dQw4w9WgXcQ",
    "title": "Rick Astley - Never Gonna Give You Up",
    "published_at": "2009-10-25T06:57:33Z",
    "duration": "PT3M33S",
    "view_count": "1400000000",
    "like_count": "16000000",
    "comment_count": "2000000"
  },
  ...
]
```

Notice that `view_count` is a string (`"1400000000"`), not a number. That's how the YouTube API returns it. Turning it into an actual integer is a Silver layer job.

---

## `save_to_json(data)` — landing the data

The last step is writing everything to a file. This is the Bronze layer — raw data saved exactly as it came from the API.

```python
os.makedirs("./data", exist_ok=True)

filename = f"youtube_data_{date.today()}.json"
file_path = f"./data/{filename}"

with open(file_path, 'w', encoding='utf-8') as json_outfile:
    json.dump(data, json_outfile, indent=4, ensure_ascii=False)
```

**`os.makedirs("./data", exist_ok=True)`** — creates the `data/` folder if it doesn't exist. The `exist_ok=True` part means it won't throw an error if the folder is already there. Without it, the second time you run the script it would crash.

**The date in the filename** (`youtube_data_2026-06-21.json`) means every daily run creates a new file instead of overwriting yesterday's. This keeps a history of snapshots.

**`indent=4`** makes the JSON human-readable — without it, the entire file would be one long line that's impossible to read.

**`ensure_ascii=False`** means characters outside of standard English — Korean titles, Arabic, emoji — get written as-is instead of being converted into ugly escape sequences like `😀`.

---

## Error handling — what happens when something goes wrong

Both API functions wrap their code in `try/except` blocks. Here's why that matters.

When you make an HTTP request, things can fail silently. If the YouTube API returns a 429 error (rate limit exceeded) or a 403 (invalid API key), `requests` by default doesn't raise an exception — it just gives you the error response and moves on. Your code would then try to access `data['items']` on an error response and crash in a confusing way.

`response.raise_for_status()` fixes this. It checks the HTTP status code and raises a proper Python exception if it's anything other than a 200 success. Now you get a clear error immediately instead of a mysterious crash three lines later.

```python
try:
    response = requests.get(url)
    response.raise_for_status()
    ...
except requests.exceptions.RequestException as e:
    print(f"HTTP request failed: {e}")
    raise
```

The `raise` at the end re-raises the exception after printing the message. This stops the program cleanly rather than letting it continue with incomplete or corrupted data.

---

## Running the script

Make sure your `.env` has `API_KEY` and `CHANNEL_HANDLE` set, then:

```bash
python video_stats.py
```

You'll see the playlist ID printed in the terminal, and a new JSON file will appear in `./data/` when it finishes.
