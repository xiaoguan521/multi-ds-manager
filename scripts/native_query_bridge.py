#!/usr/bin/env python3

import json
import sys


def format_error(exc):
    parts = []
    seen = set()
    current = exc

    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current).strip() or repr(current)
        if text:
            parts.append(text)
        current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)

    return " | caused by: ".join(parts) or repr(exc)


def load_request():
    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("empty request")
    return json.loads(raw)


def normalize_value(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): normalize_value(item) for key, item in value.items()}
    return str(value)


def rows_to_json(cursor, rows, max_rows=None):
    columns = []
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]

    items = []
    row_limit = max_rows if isinstance(max_rows, int) and max_rows > 0 else len(rows)

    for row in rows[:row_limit]:
        if isinstance(row, dict):
            items.append({str(key): normalize_value(value) for key, value in row.items()})
        else:
            item = {}
            for index, value in enumerate(row):
                column_name = columns[index] if index < len(columns) else str(index)
                item[str(column_name)] = normalize_value(value)
            items.append(item)
    return items


def handle_cursor_operation(connection, cursor, req, driver_name):
    operation_type = str(req.get("operation_type", "")).strip().lower()
    params = req.get("params") or []

    if operation_type == "query":
        cursor.execute(req["sql"], params)
        rows = cursor.fetchall()
        return {
            "ok": True,
            "driver": driver_name,
            "rows": rows_to_json(cursor, rows, req.get("max_rows")),
            "affected_rows": 0,
            "out_params": [],
            "statement": req["sql"],
        }

    if operation_type == "execute":
        cursor.execute(req["sql"], params)
        connection.commit()
        return {
            "ok": True,
            "driver": driver_name,
            "rows": [],
            "affected_rows": cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0,
            "out_params": [],
            "statement": req["sql"],
        }

    if operation_type == "procedure":
        out_params = cursor.callproc(req["procedure_name"], params)
        connection.commit()
        return {
            "ok": True,
            "driver": driver_name,
            "rows": [],
            "affected_rows": 0,
            "out_params": [normalize_value(item) for item in out_params],
            "statement": req["procedure_name"],
        }

    raise ValueError(f"unsupported operation_type: {operation_type}")


def run_oracle_query(req):
    import oracledb

    dsn = f"{req['host']}:{req['port']}/{req['database']}"
    with oracledb.connect(
        user=req["user"],
        password=req["password"],
        dsn=dsn,
    ) as connection:
        with connection.cursor() as cursor:
            return handle_cursor_operation(connection, cursor, req, "native/oracle-python")


def run_dm_query(req):
    import dmPython

    connection = dmPython.connect(
        user=req["user"],
        password=req["password"],
        server=req["host"],
        port=req["port"],
    )
    try:
        cursor = connection.cursor()
        try:
            return handle_cursor_operation(connection, cursor, req, "native/dm-python")
        finally:
            cursor.close()
    finally:
        connection.close()


def main():
    try:
        req = load_request()
        db_type = str(req.get("db_type", "")).strip().lower()

        if db_type == "oracle":
            result = run_oracle_query(req)
        elif db_type == "dm":
            result = run_dm_query(req)
        else:
            raise ValueError(f"unsupported db_type for native bridge: {db_type}")

        print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": format_error(exc),
                },
                ensure_ascii=False,
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
