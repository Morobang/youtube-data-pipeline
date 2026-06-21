import re
from datetime import datetime, timedelta


def parse_duration(duration_str):
    """
    Parse an ISO 8601 duration string into a timedelta.

    YouTube returns durations like 'PT5M30S' (5 minutes 30 seconds),
    'PT1H2M3S' (1 hour 2 minutes 3 seconds), or 'PT45S' (45 seconds).
    Uses regex to extract each component safely.

    Args:
        duration_str (str): ISO 8601 duration, e.g. 'PT5M30S'.

    Returns:
        timedelta: The parsed duration.
    """
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str or '')
    if not match:
        return timedelta(0)

    hours   = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def transform_data(row):
    """
    Transform a bronze row into a core row.

    Changes made:
    - duration: 'PT5M30S' → 'HH:MM:SS' string for the core table
    - video_type: 'Shorts' if duration <= 60 seconds, otherwise 'Normal'
    - view_count / like_count / comment_count: cast from string to int

    Args:
        row (dict): A row dict from bronze.yt_api (RealDictCursor result).

    Returns:
        dict: The same dict with transformed values.
    """
    row = dict(row)  # make a mutable copy so we don't modify the original

    duration = parse_duration(row.get('duration', ''))

    row['duration']   = (datetime.min + duration).time().strftime('%H:%M:%S')
    row['video_type'] = "Shorts" if duration.total_seconds() <= 60 else "Normal"

    # Cast string counts to integers (bronze stores them as strings)
    row['view_count']    = int(row['view_count'])    if row.get('view_count')    else 0
    row['like_count']    = int(row['like_count'])    if row.get('like_count')    else 0
    row['comment_count'] = int(row['comment_count']) if row.get('comment_count') else 0

    return row
