from datetime import date, timedelta
from typing import List, Dict, Any
from collections import defaultdict
from datetime import time as dt_time

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession


from dotenv import load_dotenv

from models import Tracking, User, Event, Track

load_dotenv(override=True)


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


async def get_tracking_results_sqlalchemy(
    session: AsyncSession,
    gender: str,
    start_period: date,
    end_period: date,
    order_by: str = "start",
    group_rounds: str = "none",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Aggregiert Tracking-Ergebnisse nach verschiedenen Gruppierungsmodi."""
    conditions = [
        func.date(Tracking.start_date_time) >= start_period,
        func.date(Tracking.start_date_time) <= end_period,
    ]
    if gender:
        conditions.append(User.gender == gender)
    if group_rounds == "all":
        stmt = (
            select(
                User.username,
                func.sum(Track.distanz).label("km_total"),
                func.sum(func.time_to_sec(Tracking.time)).label("time_total"),
                func.count(Tracking.tracking_id).label("rounds"),
            )
            .join(User, Tracking.user_id == User.user_id)
            .join(Track, Tracking.track_id == Track.track_id)
            .where(and_(*conditions))
            .group_by(User.username)
            .order_by(func.sum(Track.distanz).desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return [dict(row._mapping) for row in result.fetchall()]
    order_clause = (
        Tracking.start_date_time.desc() if order_by == "start" else Tracking.time.asc()
    )
    stmt = (
        select(
            Tracking.tracking_id,
            Tracking.start_date_time,
            func.time_to_sec(Tracking.time).label("time"),
            Track.distanz.label("km"),
            Event.name.label("event_name"),
            User.username,
        )
        .join(User, Tracking.user_id == User.user_id)
        .outerjoin(Event, Tracking.event_id == Event.event_id)
        .join(Track, Tracking.track_id == Track.track_id)
        .where(and_(*conditions))
        .order_by(order_clause)
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.fetchall()
    return [dict(row._mapping) for row in rows]


async def get_tracking_results_python(
    session: AsyncSession,
    gender: str,
    start_period: date,
    end_period: date,
    order_by: str = "start",
    group_rounds: str = "none",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Aggregiert Tracking-Ergebnisse in Python nach verschiedenen Gruppierungsmodi."""
    conditions = [
        func.date(Tracking.start_date_time) >= start_period,
        func.date(Tracking.start_date_time) <= end_period,
    ]
    if gender:
        conditions.append(User.gender == gender)

    stmt = (
        select(
            Tracking.tracking_id,
            Tracking.start_date_time,
            Tracking.time,
            Track.distanz.label("km"),
            Event.name.label("event_name"),
            User.username,
        )
        .join(User, Tracking.user_id == User.user_id)
        .outerjoin(Event, Tracking.event_id == Event.event_id)
        .join(Track, Tracking.track_id == Track.track_id)
        .where(and_(*conditions))
    )

    result = await session.execute(stmt)
    rows = [dict(row._mapping) for row in result.fetchall()]

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
