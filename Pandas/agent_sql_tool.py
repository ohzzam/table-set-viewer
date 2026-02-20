import os
import json
import time
import re
from typing import Any, Dict, Tuple, Optional, List
from datetime import datetime

import pymysql


DENY_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|"
    r"LOAD\s+DATA|INTO\s+OUTFILE|INFILE|SET|LOCK|UNLOCK|KILL)\b",
    re.IGNORECASE
)

ALLOW_PATTERN = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)


def json_serializer(obj):
    """JSON 직렬화를 위한 헬퍼 함수"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_provider(cfg: Dict[str, Any], provider_name: str) -> Dict[str, Any]:
    providers = cfg["mcp"]["context_providers"]
    for p in providers:
        if p["name"] == provider_name:
            return p
    raise KeyError(f"provider not found: {provider_name}")


def get_tool(cfg: Dict[str, Any], tool_id: str) -> Dict[str, Any]:
    tools = cfg["mcp"]["tools"]
    for t in tools:
        if t["tool_id"] == tool_id:
            return t
    raise KeyError(f"tool not found: {tool_id}")


def get_query_template(cfg: Dict[str, Any], template_id: str) -> Dict[str, Any]:
    for qt in cfg["mcp"]["query_templates"]:
        if qt["id"] == template_id:
            return qt
    raise KeyError(f"query template not found: {template_id}")


def validate_sql(sql: str) -> None:
    if not ALLOW_PATTERN.search(sql):
        raise ValueError("SQL is not allowed: only SELECT/WITH/EXPLAIN are permitted.")
    if DENY_PATTERN.search(sql):
        raise ValueError("SQL contains denied keyword(s).")


def validate_params(qt: Dict[str, Any], params: Dict[str, Any]) -> None:
    spec = qt.get("params", {})
    for k, rule in spec.items():
        if rule.get("required") and k not in params:
            raise ValueError(f"missing required param: {k}")

        if k not in params:
            continue

        v = params[k]
        t = rule.get("type")
        if t == "int":
            if not isinstance(v, int):
                raise ValueError(f"param {k} must be int")
            if "min" in rule and v < rule["min"]:
                raise ValueError(f"param {k} must be >= {rule['min']}")
            if "max" in rule and v > rule["max"]:
                raise ValueError(f"param {k} must be <= {rule['max']}")
        elif t == "string":
            if not isinstance(v, str):
                raise ValueError(f"param {k} must be string")
            if "max_len" in rule and len(v) > rule["max_len"]:
                raise ValueError(f"param {k} length must be <= {rule['max_len']}")
        else:
            raise ValueError(f"unsupported param type: {t}")


def connect_db(provider: Dict[str, Any]) -> pymysql.connections.Connection:
    conn_info = provider["connection"]
    pw_env = conn_info["password_env"]
    password = os.getenv(pw_env)
    if not password:
        raise RuntimeError(f"missing DB password env: {pw_env}")

    return pymysql.connect(
        host=conn_info["host"],
        port=int(conn_info["port"]),
        user=conn_info["user"],
        password=password,
        database=conn_info["database"],
        charset=conn_info.get("charset", "utf8mb4"),
        autocommit=bool(conn_info.get("autocommit", True)),
        connect_timeout=int(conn_info.get("connect_timeout_sec", 5)),
        read_timeout=int(conn_info.get("read_timeout_sec", 30)),
        write_timeout=int(conn_info.get("write_timeout_sec", 30)),
        cursorclass=pymysql.cursors.DictCursor
    )


def write_audit_log(
    conn: pymysql.connections.Connection,
    audit_cfg: Dict[str, Any],
    event: Dict[str, Any]
) -> None:
    if not audit_cfg.get("enabled"):
        return
    if audit_cfg.get("sink") != "db_table":
        return

    table = audit_cfg["table"]
    # 필드 리스트를 고정(화이트리스트)
    fields = audit_cfg["fields"]
    cols = ", ".join(fields)
    placeholders = ", ".join([f"%({f})s" for f in fields])

    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, event)


def run_query(
    cfg_path: str,
    user_id: str,
    tool_id: str,
    template_id: str,
    params: Dict[str, Any]
) -> Dict[str, Any]:
    cfg = load_config(cfg_path)
    tool = get_tool(cfg, tool_id)

    # 정책: 템플릿 ID 필수
    if tool["policy"].get("require_query_template_id") and not template_id:
        raise ValueError("template_id is required by policy.")

    qt = get_query_template(cfg, template_id)
    sql = qt["sql"]

    # 정책 검증
    validate_sql(sql)
    validate_params(qt, params)

    provider_name = tool["provider"]
    provider = get_provider(cfg, provider_name)

    started = time.time()
    success = False
    error_msg = None
    row_count = 0

    try:
        conn = connect_db(provider)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

                # max_rows 정책
                max_rows = int(tool["policy"].get("max_rows", 5000))
                if len(rows) > max_rows:
                    rows = rows[:max_rows]

                row_count = len(rows)
                success = True

                result = {
                    "agent_id": cfg["agent_id"],
                    "tool_id": tool_id,
                    "query_template_id": template_id,
                    "row_count": row_count,
                    "data": rows
                }

            # 감사로그(동일 커넥션 사용)
            elapsed_ms = int((time.time() - started) * 1000)
            audit_event = {
                "event_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "agent_id": cfg["agent_id"],
                "user_id": user_id,
                "tool_id": tool_id,
                "query_template_id": template_id,
                "params_json": json.dumps(params, ensure_ascii=False),
                "row_count": row_count,
                "elapsed_ms": elapsed_ms,
                "success": 1 if success else 0,
                "error_msg": error_msg
            }
            write_audit_log(conn, tool["audit"], audit_event)

        return result

    except Exception as e:
        elapsed_ms = int((time.time() - started) * 1000)
        error_msg = str(e)

        # 실패 시에도 가능하면 감사로그 남김(연결 실패면 불가)
        try:
            conn = connect_db(provider)
            with conn:
                audit_event = {
                    "event_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "agent_id": cfg["agent_id"],
                    "user_id": user_id,
                    "tool_id": tool_id,
                    "query_template_id": template_id,
                    "params_json": json.dumps(params, ensure_ascii=False),
                    "row_count": 0,
                    "elapsed_ms": elapsed_ms,
                    "success": 0,
                    "error_msg": error_msg
                }
                write_audit_log(conn, tool["audit"], audit_event)
        except Exception:
            pass

        raise


if __name__ == "__main__":
    # 예시 실행:
    # export dhwoan="dhwoan"
    import os
    os.environ['dhwoan'] = 'dhwoan'
    
    out = run_query(
        cfg_path="C:/Python/Pandas/agent_mariadb_mcp.json",
        user_id="u0001",
        tool_id="sql_query_readonly",
        template_id="Q002_audit_log_recent",
        params={"limit_rows": 10}
    )
    print(json.dumps(out, ensure_ascii=False, indent=2, default=json_serializer))
