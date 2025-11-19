from datetime import timedelta, datetime

def parse_duration(duration_str):
    """
    Parse an ISO 8601 YouTube duration string, e.g., 'PT1H2M3S', 'PT15M', 'PT45S'.
    Returns a Python timedelta.
    """

    # Remove 'P' and leading 'T'
    duration_str = duration_str.replace("P", "").replace("T", "")

    # Initialize values
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    # Parse each component safely
    for unit in values.keys():
        if unit in duration_str:
            # Split only once so the rest continues to be parsable
            num, duration_str = duration_str.split(unit, 1)
            values[unit] = int(num)

    return timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"],
    )


def transform_data(row):
    """
    Transform a YouTube API row:
    - Convert duration to HH:MM:SS time
    - Add video_Type = 'Shorts' if <= 60s else 'Normal'
    """

    # Parse duration into timedelta
    duration_td = parse_duration(row["Duration"])

    # Convert timedelta → time (duration always < 24h for YouTube)
    row["duration"] = (datetime.min + duration_td).time()

    # Tag shorts
    row["video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
