import time
from mongo.sqlalchemy_filter import (
    get_tracking_results_sqlalchemy,
    get_tracking_results_python,
)
from dotenv import load_dotenv

load_dotenv(override=True)

from sqlalchemy import update
from common.models import User
from datetime import time as dt_time, timedelta


def to_seconds(val):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, timedelta):
        return val.total_seconds()
    if isinstance(val, dt_time):
        return val.hour * 3600 + val.minute * 60 + val.second
    if isinstance(val, str):
        parts = val.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0


def floats_almost_equal(a, b, tol=1e-6):
    a_sec = to_seconds(a)
    b_sec = to_seconds(b)
    return abs(a_sec - b_sec) < tol


def results_almost_equal(list1, list2, float_keys=("km_total", "time_total"), tol=1e-6):
    if len(list1) != len(list2):
        print(f"Längen unterschiedlich: SQL={len(list1)}, Python={len(list2)}")
        return False
    for i, (row1, row2) in enumerate(zip(list1, list2)):
        for key in float_keys:
            if key in row1 and key in row2:
                if not floats_almost_equal(row1[key], row2[key], tol):
                    print(
                        f"Unterschied bei Index {i}, Feld '{key}': SQL={row1[key]}, Python={row2[key]}"
                    )
                    return False
        for key in row1:
            if key not in float_keys and row1[key] != row2.get(key):
                print(
                    f"Unterschied bei Index {i}, Feld '{key}': SQL={row1[key]}, Python={row2.get(key)}"
                )
                return False
    return True


async def benchmark_functions(
    session,
    gender: str,
    start_period,
    end_period,
    group_rounds,
    order_by,
    limit,
    variant="sql",
):
    t1 = time.perf_counter()
    if variant == "sql":
        res = await get_tracking_results_sqlalchemy(
            session, gender, start_period, end_period, order_by, group_rounds, limit
        )
    else:
        res = await get_tracking_results_python(
            session, gender, start_period, end_period, order_by, group_rounds, limit
        )
    t2 = time.perf_counter()

    duration = t2 - t1
    result_count = len(res)
    return {
        "variant": variant,
        "duration": duration,
        "result_count": result_count,
        "equal": None,
    }


async def benchmark_update_username_sqlalchemy(session, user_id, new_username):
    t1 = time.perf_counter()
    await session.execute(
        update(User).where(User.user_id == user_id).values(username=new_username)
    )
    await session.commit()
    t2 = time.perf_counter()
    return t2 - t1


async def benchmark_update_gender_sqlalchemy(session, user_id, new_gender):
    t1 = time.perf_counter()
    await session.execute(
        update(User).where(User.user_id == user_id).values(gender=new_gender)
    )
    await session.commit()
    t2 = time.perf_counter()
    return t2 - t1
