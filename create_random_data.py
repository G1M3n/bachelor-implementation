import random
import string
import uuid
from datetime import datetime, timedelta, date, time as dt_time


def random_str(length=8):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_time():
    return dt_time(minute=random.randint(10, 30), second=random.randint(0, 59))


def random_date(start_year=2022, end_year=2025):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def time_to_seconds(val):
    if isinstance(val, dt_time):
        return val.hour * 3600 + val.minute * 60 + val.second
    return float(val)


def generate_synchronized_testdata(
    n_users=100, n_tracks=10, n_events=5, n_trackings=1000
):
    users = []
    for _ in range(n_users):
        user_id = str(uuid.uuid4())
        users.append(
            {
                "user_id": user_id,
                "username": f"user_{random_str(12)}",
                "first_name": random_str(5).capitalize(),
                "last_name": random_str(7).capitalize(),
                "gender": random.choice(["male", "female", "other", "unknown"]),
                "email": f"{random_str(12)}@test.com",
                "birthday": str(random_date(1970, 2010)),
                "hashed_password": random_str(32),
            }
        )

    tracks = []
    for _ in range(n_tracks):
        track_id = str(uuid.uuid4())
        tracks.append(
            {
                "track_id": track_id,
                "name": f"Track_{random_str(4)}",
                "km": round(random.uniform(0.4, 10.0), 2),
                "activ": True,
            }
        )

    events = []
    for _ in range(n_events):
        event_id = str(uuid.uuid4())
        events.append(
            {
                "event_id": event_id,
                "name": f"Event_{random_str(5)}",
                "start": datetime.utcnow() - timedelta(days=random.randint(0, 365)),
                "end": datetime.utcnow() + timedelta(days=random.randint(0, 365)),
            }
        )

    trackings = []
    for _ in range(n_trackings):
        user = random.choice(users)
        track = random.choice(tracks)
        event = random.choice(events)
        tracking_time = random_time()
        trackings.append(
            {
                "tracking_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "track_id": track["track_id"],
                "event_id": event["event_id"],
                "username": user["username"],
                "gender": user["gender"],
                "km": track["km"],
                "event_name": event["name"],
                "start_date_time": datetime.utcnow()
                - timedelta(days=random.randint(0, 730)),
                "time": tracking_time.strftime("%H:%M:%S"),
                "time_seconds": time_to_seconds(tracking_time),
            }
        )

    return users, tracks, events, trackings
