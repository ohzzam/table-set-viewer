import json
import pymysql
import os
from datetime import datetime

# 1. DB 연결

conn = pymysql.connect(
# zipfile 모듈 임포트
        host="127.0.0.1",
        user="root",
        password="dhwoan",
        database="agri_data",
        charset="utf8mb4",
        autocommit=False
    )
cursor = conn.cursor()

# 2. RAW JSON 로드

# JSON 파일이 들어있는 폴더 경로
json_dir = r"C:\Users/NC627/Downloads/097.지능형 스마트팜 통합 데이터(토마토)/01.데이터/1.Training/라벨링데이터/" \



# 모든 하위 폴더의 json 파일까지 재귀적으로 처리
for root, dirs, files in os.walk(json_dir):
    for filename in files:
        if filename.lower().endswith('.json'):
            file_path = os.path.join(root, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_json = json.load(f)

            # 3. RAW 식별자 생성 (week+number+type+image번호 조합)
            image_path = raw_json.get("imagePath")
            image_num = image_path.split('_')[-1].split('.')[0]
            fa = raw_json.get('file_attributes', {})
            week = fa.get('week', 'unknown')
            number = fa.get('number', 'unknown')
            type_ = fa.get('type', 'unknown')
            raw_id = f"RAW_{week}{number}{type_}_{image_num}"
            farm_id = raw_json.get("farmId", "UNKNOWN")

            # 4. RAW 원본 적재 (중복 체크)
            check_sql = "SELECT EXISTS(SELECT 1 FROM raw_image_data WHERE raw_id=%s)"
            cursor.execute(check_sql, (raw_id,))
            exists = cursor.fetchone()[0]
            if exists:
                print(f"{raw_id} already exists. Skipping.")
                continue
            else:
                print(f"{raw_id} 개 적재 중..")
            sql_raw = """
            INSERT INTO raw_image_data (raw_id, farm_id, image_path, ingest_ts, raw_payload)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(
                sql_raw,
                (
                    raw_id,
                    farm_id,
                    image_path,
                    datetime.now(),
                    json.dumps(raw_json, ensure_ascii=False)
                )
            )

            # 5. 이미지 메타 적재
            sql_image = """
            INSERT INTO image_meta (raw_id, farm_id, image_path, image_width, image_height, capture_dt)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            # 파일명에서 날짜 추출 (예: V001_tom1_39_012_f_09_20210930_14_00122404_49122255.json)
            image_filename = image_path.split('/')[-1] if '/' in image_path else image_path
            import re
            m = re.search(r'(\d{8})', image_filename)
            date_str = m.group(1) if m else None
            try:
                capture_date = datetime.strptime(date_str, "%Y%m%d") if date_str else datetime.now()
            except:
                capture_date = datetime.now()

            cursor.execute(
                sql_image,
                (
                    raw_id,
                    farm_id,
                    image_path,
                    raw_json.get("imageWidth"),
                    raw_json.get("imageHeight"),
                    capture_date
                )
            )

            # 6. 라벨 객체 적재
            sql_label = """
            INSERT INTO label_object (raw_id, object_type, polygon_json, quality_flag)
            VALUES (%s, %s, %s, %s)
            """
            for shape in raw_json.get("shapes", []):
                cursor.execute(
                    sql_label,
                    (
                        raw_id,
                        shape.get("label"),
                        json.dumps(shape.get("points")),
                        "N"
                    )
                )

            # 7. 생육 지표 적재
            sql_growth = """
            INSERT INTO growth_indicator (raw_id, flowering_node, flower_count)
            VALUES (%s, %s, %s)
            """
            gi = raw_json.get("growth_indicators", {})
            cursor.execute(
                sql_growth,
                (
                    raw_id,
                    gi.get("floweringNode"),
                    gi.get("numberOfTheFlower")
                )
            )

# 8. 커밋
conn.commit()

# 9. 종료
cursor.close()
conn.close()

print("RAW 데이터 적재 완료")
