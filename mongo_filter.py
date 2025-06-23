from datetime import time as dt_time, timedelta, date, datetime

import polars as pl
from collections import defaultdict
import pandas as pd


def time_to_seconds(val):
    if isinstance(val, timedelta):
        return val.total_seconds()
    if isinstance(val, dt_time):
        return val.hour * 3600 + val.minute * 60 + val.second
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        parts = val.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0


async def get_tracking_results_mongodb(
    db,
    gender: str,
    start_period: date,
    end_period: date,
    order_by: str = "start",
    group_rounds: str = "none",
    limit: int = 100,
):
    match_stage = {
        "start_date_time": {
            "$gte": datetime.combine(start_period, datetime.min.time()),
            "$lte": datetime.combine(end_period, datetime.max.time()),
        }
    }
    if gender:
        match_stage["gender"] = gender
    if group_rounds == "all":
        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": "$username",
                    "km_total": {"$sum": "$km"},
                    "time_total": {"$sum": {"$toDouble": "$time_seconds"}},
                    "rounds": {"$sum": 1},
                }
            },
            {"$sort": {"km_total": -1}},
            {"$limit": limit},
        ]
        cursor = db.tracking.aggregate(pipeline)
        results = []
        async for doc in cursor:
            results.append(
                {
                    "username": doc["_id"],
                    "km_total": doc["km_total"],
                    "time_total": doc["time_total"],
                    "rounds": doc["rounds"],
                }
            )
        return results
    sort_field = "start_date_time" if order_by == "start" else "time_seconds"
    sort_dir = -1 if order_by == "start" else 1
    cursor = db.tracking.find(match_stage).sort(sort_field, sort_dir).limit(limit)
    results = []
    async for doc in cursor:
        results.append(
            {
                "tracking_id": doc.get("tracking_id"),
                "start_date_time": doc.get("start_date_time"),
                "time": doc.get("time_seconds"),
                "km": doc.get("km"),
                "event_name": doc.get("event_name"),
                "username": doc.get("username"),
            }
        )
    return results


async def get_tracking_results_mongodb_python(
    db,
    gender: str,
    start_period: date,
    end_period: date,
    order_by: str = "start",
    group_rounds: str = "none",
    limit: int = 100,
):
    match_stage = {
        "start_date_time": {
            "$gte": datetime.combine(start_period, datetime.min.time()),
            "$lte": datetime.combine(end_period, datetime.max.time()),
        }
    }
    if gender:
        match_stage["gender"] = gender

    cursor = db.tracking.find(match_stage)
    rows = []
    async for doc in cursor:
        rows.append(
            {
                "tracking_id": doc.get("tracking_id"),
                "start_date_time": doc.get("start_date_time"),
                "time": doc.get("time_seconds"),
                "km": doc.get("km"),
                "event_name": doc.get("event_name"),
                "username": doc.get("username"),
            }
        )
    if not rows:
        return []
    if group_rounds == "all":
        grouped = defaultdict(
            lambda: {"username": None, "km_total": 0, "time_total": 0, "rounds": 0}
        )
        for row in rows:
            username = row["username"]
            time_sec = time_to_seconds(row["time"])
            if grouped[username]["username"] is None:
                grouped[username]["username"] = username
            grouped[username]["km_total"] += row["km"]
            grouped[username]["time_total"] += time_sec
            grouped[username]["rounds"] += 1
        result_list = list(grouped.values())
        result_list.sort(key=lambda g: g["km_total"], reverse=True)
        return result_list[:limit]

    if group_rounds == "behind":
        rows.sort(key=lambda r: (r["username"], r["start_date_time"]))
        results = []
        current_group = None
        for row in rows:
            time_sec = time_to_seconds(row["time"])
            if (not current_group) or (current_group["username"] != row["username"]):
                if current_group:
                    results.append(current_group)
                current_group = row.copy()
                current_group["time"] = time_sec
                current_group["rounds"] = 1
            else:
                group_end = current_group["start_date_time"] + timedelta(
                    seconds=current_group["time"]
                )
                time_diff = abs((group_end - row["start_date_time"]).total_seconds())
                if time_diff <= 1:
                    current_group["time"] += time_sec
                    current_group["rounds"] += 1
                else:
                    results.append(current_group)
                    current_group = row.copy()
                    current_group["time"] = time_sec
                    current_group["rounds"] = 1
        if current_group:
            results.append(current_group)
        if order_by == "start":
            results.sort(key=lambda g: g["start_date_time"], reverse=True)
        else:
            results.sort(key=lambda g: (-g["rounds"], g["time"]))
        return results[:limit]

    if group_rounds == "none":
        if order_by == "start":
            rows.sort(key=lambda r: r["start_date_time"], reverse=True)
        else:
            rows.sort(key=lambda r: time_to_seconds(r["time"]))
        return rows[:limit]
