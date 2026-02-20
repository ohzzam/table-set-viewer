import pymysql
import json

# DB 연결
conn = pymysql.connect(
# zipfile 모듈 임포트
        host="127.0.0.1",
        user="root",
        password="dhwoan",
        database="agri_data",
        charset="utf8mb4",
        autocommit=False
    )
cursor = conn.cursor(pymysql.cursors.DictCursor)

# =========================
# 1. 이미지 메타 품질 판정
# =========================
cursor.execute("""
SELECT image_id, raw_id, image_width, image_height
FROM image_meta
""")

for row in cursor.fetchall():
    if row["image_width"] is None or row["image_height"] is None:
        # UPSERT: 이미 있으면 UPDATE, 없으면 INSERT
        cursor.execute("""
        SELECT COUNT(*) FROM dq_result WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
        """, (row["raw_id"], "IMAGE_META", row["image_id"], "IMG_001"))
        exists = cursor.fetchone()["COUNT(*)"]
        if exists:
            cursor.execute("""
                UPDATE dq_result SET quality_flag=%s, message=%s
                WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
            """, (
                "M",
                "이미지 해상도 정보 누락",
                row["raw_id"],
                "IMAGE_META",
                row["image_id"],
                "IMG_001"
            ))
        else:
            cursor.execute("""
                INSERT INTO dq_result
                (raw_id, target_table, target_id, rule_code, quality_flag, message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row["raw_id"],
                "IMAGE_META",
                row["image_id"],
                "IMG_001",
                "M",
                "이미지 해상도 정보 누락"
            ))

# =========================
# 2. 라벨 Polygon 품질 판정
# =========================
cursor.execute("""
SELECT label_id, raw_id, polygon_json
FROM label_object
""")

for row in cursor.fetchall():
    points = json.loads(row["polygon_json"])

    # 규칙 POLY_001: 최소 좌표 수
    if len(points) < 3:
        cursor.execute("""
        SELECT COUNT(*) FROM dq_result WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
        """, (row["raw_id"], "LABEL_OBJECT", row["label_id"], "POLY_001"))
        exists = cursor.fetchone()["COUNT(*)"]
        if exists:
            cursor.execute("""
                UPDATE dq_result SET quality_flag=%s, message=%s
                WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
            """, (
                "O",
                "Polygon 좌표 수 부족",
                row["raw_id"],
                "LABEL_OBJECT",
                row["label_id"],
                "POLY_001"
            ))
        else:
            cursor.execute("""
                INSERT INTO dq_result
                (raw_id, target_table, target_id, rule_code, quality_flag, message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row["raw_id"],
                "LABEL_OBJECT",
                row["label_id"],
                "POLY_001",
                "O",
                "Polygon 좌표 수 부족"
            ))

# =========================
# 3. 생육 지표 품질 판정
# =========================
cursor.execute("""
SELECT growth_id, raw_id, flower_count
FROM growth_indicator
""")

for row in cursor.fetchall():
    if row["flower_count"] is not None and row["flower_count"] < 0:
        cursor.execute("""
        SELECT COUNT(*) FROM dq_result WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
        """, (row["raw_id"], "METRIC", row["growth_id"], "GROW_001"))
        exists = cursor.fetchone()["COUNT(*)"]
        if exists:
            cursor.execute("""
                UPDATE dq_result SET quality_flag=%s, message=%s
                WHERE raw_id=%s AND target_table=%s AND target_id=%s AND rule_code=%s
            """, (
                "O",
                "꽃 개수 음수 값",
                row["raw_id"],
                "METRIC",
                row["growth_id"],
                "GROW_001"
            ))
        else:
            cursor.execute("""
                INSERT INTO dq_result
                (raw_id, target_table, target_id, rule_code, quality_flag, message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row["raw_id"],
                "METRIC",
                row["growth_id"],
                "GROW_001",
                "O",
                "꽃 개수 음수 값"
            ))

# 커밋
conn.commit()

cursor.close()
conn.close()

print("품질 판정(DQ) 완료")
