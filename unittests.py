import pytest
from datetime import datetime, date, timedelta
from collections import defaultdict
from types import SimpleNamespace

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sqlalchemy_filter as sa_filter
import mongo_filter as mongo_filter


class DummySession:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, stmt):
        class Result:
            def fetchall(self_inner):
                return [SimpleNamespace(_mapping=row) for row in self._rows]

        return Result()


class DummyCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, *args, **kwargs):
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    async def __aiter__(self):
        for doc in self.docs:
            yield doc


class DummyMongoDB:
    def __init__(self, docs):
        self.docs = docs
        self.tracking = self

    def find(self, match_stage):
        return DummyCursor(self.docs)

    def aggregate(self, pipeline):
        grouped = defaultdict(
            lambda: {"_id": None, "km_total": 0, "time_total": 0, "rounds": 0}
        )
        for doc in self.docs:
            username = doc["username"]
            if grouped[username]["_id"] is None:
                grouped[username]["_id"] = username
            grouped[username]["km_total"] += doc["km"]
            grouped[username]["time_total"] += doc["time_seconds"]
            grouped[username]["rounds"] += 1
        result = list(grouped.values())

        class AsyncIter:
            def __init__(self, items):
                self.items = items

            async def __aiter__(self):
                for item in self.items:
                    yield item

        return AsyncIter(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("group_rounds", ["all", "behind", "none"])
async def test_get_tracking_results_python(group_rounds):
    now = datetime(2025, 6, 3, 12, 0, 0)
    rows = [
        {
            "tracking_id": 1,
            "start_date_time": now,
            "time": "00:15:00",
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 2,
            "start_date_time": now + timedelta(seconds=900),
            "time": "00:15:00",
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 3,
            "start_date_time": now + timedelta(hours=1),
            "time": "00:20:00",
            "km": 10.0,
            "event_name": "E2",
            "username": "bob",
        },
    ]
    session = DummySession(rows)
    res = await sa_filter.get_tracking_results_python(
        session=session,
        gender=None,
        start_period=date(2025, 6, 1),
        end_period=date(2025, 6, 5),
        order_by="start",
        group_rounds=group_rounds,
        limit=10,
    )
    assert isinstance(res, list)
    if group_rounds == "all":
        assert any(r["username"] == "alice" and r["km_total"] == 10.0 for r in res)
        assert any(r["username"] == "bob" and r["km_total"] == 10.0 for r in res)
    elif group_rounds == "behind":
        assert any(r["username"] == "alice" and r["rounds"] == 2 for r in res)
    elif group_rounds == "none":
        assert len(res) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("group_rounds", ["all", "behind", "none"])
async def test_get_tracking_results_mongodb_python(group_rounds):
    now = datetime(2025, 6, 3, 12, 0, 0)
    docs = [
        {
            "tracking_id": 1,
            "start_date_time": now,
            "time": "00:15:00",
            "time_seconds": 900,
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 2,
            "start_date_time": now + timedelta(seconds=900),
            "time": "00:15:00",
            "time_seconds": 900,
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 3,
            "start_date_time": now + timedelta(hours=1),
            "time": "00:20:00",
            "time_seconds": 1200,
            "km": 10.0,
            "event_name": "E2",
            "username": "bob",
        },
    ]
    db = DummyMongoDB(docs)
    res = await mongo_filter.get_tracking_results_mongodb_python(
        db=db,
        gender=None,
        start_period=date(2025, 6, 1),
        end_period=date(2025, 6, 5),
        order_by="start",
        group_rounds=group_rounds,
        limit=10,
    )
    assert isinstance(res, list)
    if group_rounds == "all":
        assert any(r["username"] == "alice" and r["km_total"] == 10.0 for r in res)
        assert any(r["username"] == "bob" and r["km_total"] == 10.0 for r in res)
    elif group_rounds == "behind":
        print("[DEBUG] result for 'behind':", res)
        assert any(r["username"] == "alice" and r["rounds"] == 2 for r in res)
    elif group_rounds == "none":
        assert len(res) == 3


@pytest.mark.asyncio
async def test_get_tracking_results_sqlalchemy_all():
    rows = [
        {"username": "alice", "km_total": 10.0, "time_total": 1800, "rounds": 2},
        {"username": "bob", "km_total": 10.0, "time_total": 1200, "rounds": 1},
    ]
    session = DummySession(rows)
    res = await sa_filter.get_tracking_results_sqlalchemy(
        session=session,
        gender=None,
        start_period=date(2025, 6, 1),
        end_period=date(2025, 6, 5),
        order_by="best",
        group_rounds="all",
        limit=10,
    )
    print("[DEBUG] result for 'all':", res)
    assert isinstance(res, list)
    assert any(r["username"] == "alice" and r["km_total"] == 10.0 for r in res)
    assert any(r["username"] == "bob" and r["km_total"] == 10.0 for r in res)


@pytest.mark.asyncio
async def test_get_tracking_results_mongodb_all():
    now = datetime(2025, 6, 3, 12, 0, 0)
    docs = [
        {
            "tracking_id": 1,
            "start_date_time": now,
            "time": "00:15:00",
            "time_seconds": 900,
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 2,
            "start_date_time": now + timedelta(seconds=1),
            "time": "00:15:00",
            "time_seconds": 900,
            "km": 5.0,
            "event_name": "E1",
            "username": "alice",
        },
        {
            "tracking_id": 3,
            "start_date_time": now + timedelta(hours=1),
            "time": "00:20:00",
            "time_seconds": 1200,
            "km": 10.0,
            "event_name": "E2",
            "username": "bob",
        },
    ]
    db = DummyMongoDB(docs)
    res = await mongo_filter.get_tracking_results_mongodb(
        db=db,
        gender=None,
        start_period=date(2025, 6, 1),
        end_period=date(2025, 6, 5),
        order_by="start",
        group_rounds="all",
        limit=10,
    )
    assert isinstance(res, list)
    assert any(r["username"] == "alice" and r["km_total"] == 10.0 for r in res)
    assert any(r["username"] == "bob" and r["km_total"] == 10.0 for r in res)
