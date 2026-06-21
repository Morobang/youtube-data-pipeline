import requests
import json
import os
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

# --- Configuration ---
max_results = 50  # YouTube API maximum items returned per page


@task
def get_playlist_id():
    """
    Fetch the uploads playlist ID for a YouTube channel.

    The YouTube API doesn't expose a channel's videos directly — every channel
    has a hidden "uploads" playlist that contains all its public videos. This
    function looks up that playlist ID using the channel's @handle so that
    get_video_ids() can paginate through it.

    Returns:
        str: The uploads playlist ID (e.g. "UU...").

    Raises:
        KeyError / IndexError: If the API response doesn't have the expected shape.
        requests.exceptions.RequestException: If the HTTP call fails.
    """
    api_key = Variable.get("API_KEY")
    channel_handle = Variable.get("CHANNEL_HANDLE")

    try:
        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
        )
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        print(f"Playlist ID: {playlist_id}")
        return playlist_id

    except (KeyError, IndexError) as e:
        print(f"Unexpected API response structure: {e}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        raise


@task
def get_video_ids(playlist_id):
    """
    Collect all video IDs from a YouTube playlist.

    The YouTube API returns at most `max_results` items per page. When there
    are more videos, the response includes a `nextPageToken`. This function
    keeps requesting the next page until that token disappears, then returns
    the full list of video IDs.

    Args:
        playlist_id (str): The uploads playlist ID returned by get_playlist_id().

    Returns:
        list[str]: All video IDs found in the playlist.

    Raises:
        requests.exceptions.RequestException: If any HTTP call fails.
    """
    api_key = Variable.get("API_KEY")
    video_ids = []
    page_token = None
    base_url = (
        f"https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}"
    )

    try:
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails'].get('videoId')
                if video_id:
                    video_ids.append(video_id)

            page_token = data.get('nextPageToken')
            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        raise


def _batch(items, batch_size):
    """
    Split a list into chunks of at most `batch_size` items.

    The YouTube Videos API only accepts up to 50 IDs per request. This helper
    yields successive slices so that extract_video_data() can send several
    smaller requests instead of one oversized (invalid) one.

    Args:
        items (list): The list to split.
        batch_size (int): Maximum number of items per chunk.

    Yields:
        list: A slice of `items`.
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


@task
def extract_video_data(video_ids):
    """
    Fetch detailed metadata for a list of YouTube video IDs.

    For each batch of up to `max_results` IDs, a single API call retrieves the
    snippet (title, publish date), contentDetails (duration), and statistics
    (views, likes, comments). Results from all batches are merged into one list.

    Args:
        video_ids (list[str]): Video IDs collected by get_video_ids().

    Returns:
        list[dict]: One dict per video with keys:
            video_id, title, published_at, duration,
            view_count, like_count, comment_count.

    Raises:
        requests.exceptions.RequestException: If any HTTP call fails.
    """
    api_key = Variable.get("API_KEY")
    extracted_data = []

    try:
        for batch in _batch(video_ids, max_results):
            ids = ",".join(batch)
            url = (
                f"https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails&part=snippet&part=statistics&id={ids}&key={api_key}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                content_details = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet.get('title'),
                    "published_at": snippet.get('publishedAt'),
                    "duration": content_details.get('duration'),
                    "view_count": statistics.get('viewCount'),
                    "like_count": statistics.get('likeCount'),
                    "comment_count": statistics.get('commentCount'),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        raise


@task
def save_to_json(data, filename=None):
    """
    Write video data to a JSON file inside the /opt/airflow/data directory.

    The filename includes today's date so each daily run produces a new file
    rather than overwriting the previous one. The directory is created
    automatically if it doesn't exist yet.

    Args:
        data (list[dict]): The video records returned by extract_video_data().
        filename (str | None): Override the auto-generated filename. Optional.
    """
    data_dir = "/opt/airflow/data"
    os.makedirs(data_dir, exist_ok=True)

    if filename is None:
        filename = f"youtube_data_{date.today()}.json"

    file_path = f"{data_dir}/{filename}"

    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(data, json_outfile, indent=4, ensure_ascii=False)

    print(f"Saved {len(data)} records to {file_path}")
