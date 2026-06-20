import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")


def get_playlist_id():
    try:
        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        )
        response = requests.get(url)
        response.raise_for_status()  # catches 4xx/5xx HTTP errors

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
    
if __name__ == "__main__":
    print("Fetching playlist ID for channel:", CHANNEL_HANDLE)
    get_playlist_id()
else:
    print("This script is intended to be run directly, not imported.")