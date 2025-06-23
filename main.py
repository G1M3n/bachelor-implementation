import asyncio
import os
import csv
import random
from datetime import date, datetime
from dotenv import load_dotenv

from sqlalchemy import text

# SQLAlchemy/MySQL
from common.database import SessionLocal, create_tables
from create_data import insert_sqlalchemy
from sqlalchemy_benchmark import (
    benchmark_functions,
    benchmark_update_gender_sqlalchemy,
    benchmark_update_username_sqlalchemy,
)

# MongoDB/Motor
from create_mongo_data import insert_mongodb
from mongo_benchmark import (
    benchmark_mongo,
    benchmark_update_gender_mongo,
    benchmark_update_username_mongo,
)
from create_random_data import generate_synchronized_testdata
from pymongo import MongoClient

load_dotenv(override=True)

# MongoDB Setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "test_laufdaten")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]


async def clear_sqlalchemy_data():
    async with SessionLocal() as session:
        for table in ["tracking", "event", "track", "users"]:
            await session.execute(text(f"DELETE FROM {table}"))
        await session.commit()
    print("Alle SQLAlchemy-Tabellen geleert.")


async def clear_mongo_data():
    await db.tracking.delete_many({})
    await db.users.delete_many({})
    await db.tracks.delete_many({})
    await db.events.delete_many({})
    print("Alle MongoDB-Collections geleert.")


N_RUNS = 100
USER_COUNTS = [100, 500, 1000]
TRACKING_COUNTS = [1000, 10000, 50000]
BENCHMARKS = [
    ("all", "start"),
    ("none", "start"),
    ("all", "best"),
    ("none", "best"),
]
LIMIT = 100000
N_UPDATE_RUNS = 10

CSV_FILE_READ = "benchmark_results.csv"
CSV_FILE_UPDATE = "benchmark_update_results.csv"


async def insert_sqlalchemy(users, tracks, events, trackings, session):
    from common.models import User, Track, Event, Tracking
    import uuid

    session.add_all(
        [
            User(
                user_id=uuid.UUID(user["user_id"]),
                username=user["username"],
                first_name=user["first_name"],
                last_name=user["last_name"],
                gender=user["gender"],
                email=user["email"],
                birthday=user["birthday"],
                hashed_password=user["hashed_password"],
            )
            for user in users
        ]
    )
    await session.flush()
    session.add_all(
        [
            Track(
                track_id=uuid.UUID(track["track_id"]),
                name=track["name"],
                distanz=track["km"],
                activ=track["activ"],
            )
            for track in tracks
        ]
    )
    await session.flush()
    session.add_all(
        [
            Event(
                event_id=uuid.UUID(event["event_id"]),
                name=event["name"],
                start=event["start"],
                end=event["end"],
            )
            for event in events
        ]
    )
    await session.flush()
    session.add_all(
        [
            Tracking(
                tracking_id=uuid.UUID(tr["tracking_id"]),
                start_date_time=tr["start_date_time"],
                time=datetime.strptime(tr["time"], "%H:%M:%S").time(),
                user_id=uuid.UUID(tr["user_id"]),
                track_id=uuid.UUID(tr["track_id"]),
                event_id=uuid.UUID(tr["event_id"]),
            )
            for tr in trackings
        ]
    )
    await session.commit()


async def main():
    with open(CSV_FILE_READ, "w", newline="") as csvfile_read, open(
        CSV_FILE_UPDATE, "w", newline=""
    ) as csvfile_update:
        writer_read = csv.writer(csvfile_read)
        writer_read.writerow(
            [
                "timestamp",
                "db_system",
                "variant",
                "n_users",
                "n_tracks",
                "n_trackings",
                "n_events",
                "group_rounds",
                "order_by",
                "run",
                "duration",
                "result_count",
                "equal",
            ]
        )

        writer_update = csv.writer(csvfile_update)
        writer_update.writerow(
            [
                "timestamp",
                "db_system",
                "variant",
                "n_users",
                "n_tracks",
                "n_trackings",
                "n_events",
                "update_run",
                "user_id",
                "duration",
            ]
        )

        for n_users in USER_COUNTS:
            for n_trackings in TRACKING_COUNTS:
                n_tracks = 10
                n_events = 3
                for group_rounds, order_by in BENCHMARKS:
                    for run in range(1, N_RUNS + 1):
                        print(
                            f"\n=== BENCHMARK [{n_users} User, {n_trackings} Trackings, {group_rounds}, {order_by}, Run {run}] ==="
                        )
                        users, tracks, events, trackings = (
                            generate_synchronized_testdata(
                                n_users, n_tracks, n_events, n_trackings
                            )
                        )
                        user_ids = [u["user_id"] for u in users]
                        await clear_sqlalchemy_data()
                        await create_tables()
                        async with SessionLocal() as session:
                            await insert_sqlalchemy(
                                session=session,
                                users=users,
                                tracks=tracks,
                                events=events,
                                trackings=trackings,
                            )
                        print("\n--- Starte SQLAlchemy-Benchmark (DB-Filtern) ---")
                        async with SessionLocal() as session:
                            res_sql = await benchmark_functions(
                                session=session,
                                gender="male",
                                start_period=date(2010, 1, 1),
                                end_period=date(2025, 12, 31),
                                group_rounds=group_rounds,
                                order_by=order_by,
                                limit=LIMIT,
                                variant="sql",
                            )
                        writer_read.writerow(
                            [
                                datetime.now().isoformat(),
                                "SQLAlchemy",
                                "sql",
                                n_users,
                                n_tracks,
                                n_trackings,
                                n_events,
                                group_rounds,
                                order_by,
                                run,
                                res_sql.get("duration"),
                                res_sql.get("result_count"),
                                res_sql.get("equal"),
                            ]
                        )
                        csvfile_read.flush()

                        print("\n--- Starte SQLAlchemy-Benchmark (Python-Filtern) ---")
                        async with SessionLocal() as session:
                            res_py = await benchmark_functions(
                                session=session,
                                gender="male",
                                start_period=date(2010, 1, 1),
                                end_period=date(2025, 12, 31),
                                group_rounds=group_rounds,
                                order_by=order_by,
                                limit=LIMIT,
                                variant="python",
                            )
                        writer_read.writerow(
                            [
                                datetime.now().isoformat(),
                                "SQLAlchemy",
                                "python",
                                n_users,
                                n_tracks,
                                n_trackings,
                                n_events,
                                group_rounds,
                                order_by,
                                run,
                                res_py.get("duration"),
                                res_py.get("result_count"),
                                res_py.get("equal"),
                            ]
                        )
                        csvfile_read.flush()
                        await clear_mongo_data()
                        await insert_mongodb(
                            db=db,
                            users=users,
                            tracks=tracks,
                            events=events,
                            trackings=trackings,
                        )
                        print("\n--- Starte MongoDB-Benchmark (Aggregation) ---")
                        res_mongo_agg = await benchmark_mongo(
                            db=db,
                            gender="male",
                            group_rounds=group_rounds,
                            order_by=order_by,
                            limit=LIMIT,
                            variant="mongo_agg",
                        )
                        writer_read.writerow(
                            [
                                datetime.now().isoformat(),
                                "MongoDB",
                                "mongo_agg",
                                n_users,
                                n_tracks,
                                n_trackings,
                                n_events,
                                group_rounds,
                                order_by,
                                run,
                                res_mongo_agg.get("duration"),
                                res_mongo_agg.get("result_count"),
                                res_mongo_agg.get("equal"),
                            ]
                        )
                        csvfile_read.flush()

                        print("\n--- Starte MongoDB-Benchmark (Python-Filtern) ---")
                        res_mongo_py = await benchmark_mongo(
                            db=db,
                            gender="male",
                            group_rounds=group_rounds,
                            order_by=order_by,
                            limit=LIMIT,
                            variant="mongo_python",
                        )
                        writer_read.writerow(
                            [
                                datetime.now().isoformat(),
                                "MongoDB",
                                "mongo_python",
                                n_users,
                                n_tracks,
                                n_trackings,
                                n_events,
                                group_rounds,
                                order_by,
                                run,
                                res_mongo_py.get("duration"),
                                res_mongo_py.get("result_count"),
                                res_mongo_py.get("equal"),
                            ]
                        )
                        csvfile_read.flush()

                        print("\n--- Starte UPDATE-Benchmarks ---")
                        test_user_id = random.choice(user_ids)
                        for update_run in range(1, N_UPDATE_RUNS + 1):
                            new_username = (
                                f"bench_user_{update_run}_{random.randint(1, 10000)}"
                            )
                            async with SessionLocal() as session:
                                dur_sql = await benchmark_update_username_sqlalchemy(
                                    session, test_user_id, new_username
                                )
                            writer_update.writerow(
                                [
                                    datetime.now().isoformat(),
                                    "SQLAlchemy",
                                    "update_username",
                                    n_users,
                                    n_tracks,
                                    n_trackings,
                                    n_events,
                                    update_run,
                                    test_user_id,
                                    dur_sql,
                                ]
                            )
                            csvfile_update.flush()
                            new_gender = random.choice(
                                ["male", "female", "other", "unknown"]
                            )
                            async with SessionLocal() as session:
                                dur_sql = await benchmark_update_gender_sqlalchemy(
                                    session, test_user_id, new_gender
                                )
                            writer_update.writerow(
                                [
                                    datetime.now().isoformat(),
                                    "SQLAlchemy",
                                    "update_gender",
                                    n_users,
                                    n_tracks,
                                    n_trackings,
                                    n_events,
                                    update_run,
                                    test_user_id,
                                    dur_sql,
                                ]
                            )
                            csvfile_update.flush()
                            dur_mongo = await benchmark_update_username_mongo(
                                db, test_user_id, new_username
                            )
                            writer_update.writerow(
                                [
                                    datetime.now().isoformat(),
                                    "MongoDB",
                                    "update_username",
                                    n_users,
                                    n_tracks,
                                    n_trackings,
                                    n_events,
                                    update_run,
                                    test_user_id,
                                    dur_mongo,
                                ]
                            )
                            csvfile_update.flush()
                            dur_mongo = await benchmark_update_gender_mongo(
                                db, test_user_id, new_gender
                            )
                            writer_update.writerow(
                                [
                                    datetime.now().isoformat(),
                                    "MongoDB",
                                    "update_gender",
                                    n_users,
                                    n_tracks,
                                    n_trackings,
                                    n_events,
                                    update_run,
                                    test_user_id,
                                    dur_mongo,
                                ]
                            )
                            csvfile_update.flush()


if __name__ == "__main__":
    asyncio.run(main())
