#!/usr/bin/env python3
import os

import psycopg2


def delete_expired_measurements(conn, retention_hours=None):
    hours = retention_hours or int(
        os.getenv("MEASUREMENT_RETENTION_HOURS", "72")
    )
    if hours <= 0:
        raise ValueError("MEASUREMENT_RETENTION_HOURS must be positive")
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM air.measurements
            WHERE ts < CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
            """,
            (hours,),
        )
        deleted = cur.rowcount
    conn.commit()
    print(
        f"[retention] deleted {deleted} measurements older than {hours} hours"
    )
    return deleted


def main():
    conn = psycopg2.connect(
        host=os.environ["DBHOST"],
        dbname=os.environ["DBNAME"],
        user=os.environ["DBUSER"],
        password=os.environ["DBPASS"],
    )
    try:
        return delete_expired_measurements(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
