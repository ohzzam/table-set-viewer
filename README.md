# table-set-viewer

## 소개

DB 접근 정보를 입력하면 해당 데이터베이스의 테이블 목록, 테이블 구조, 컬럼 정보, PK/FK/인덱스 정보를 GUI로 조회하고, 엑셀로 내보낼 수 있는 Python 기반 툴입니다.

- 지원 DB: MySQL, PostgreSQL, Oracle, Cubrid
- 주요 기능:
  - DB 접속 및 테이블 목록/구조/코멘트/PK/FK/인덱스 정보 조회
  - 여러 테이블 동시 선택 및 구조/정의서 엑셀 내보내기
  - 테이블별 정의서(양식) 자동 생성 및 스타일 적용
  - 접속 정보 저장/불러오기
  - DDL 생성 및 복사

## 실행 방법

1. Python 3.8 이상 설치
2. 패키지 설치:
   ```bash
   pip install -r requirements.txt
   ```
   (Oracle: `pip install cx_Oracle`, Cubrid: `pip install CUBRID-Python` 필요)
3. 실행:
   ```bash
   python TABLE_SET/main.py
   ```

## 엑셀 내보내기 예시
- "테이블 목록" 시트: 전체 테이블 리스트
- "테이블 정의서" 시트: 테이블별 구조/PK/FK/인덱스/컬럼정보, 스타일 적용

## 2026-02-23 업데이트(성능/응답없음 개선)

테이블 수/컬럼 수가 많은 DB에서 “전체선택/다중 선택” 또는 “엑셀로 내보내기” 시 UI가 멈추며 Windows에서 “응답 없음”으로 보이던 문제를 개선했습니다.

- 엑셀 내보내기: DB 조회/엑셀 생성·저장을 백그라운드(QThread)로 처리하고 진행 상태를 표시
- 테이블 구조 표시: 스키마 조회를 백그라운드로 처리 + 연속 체크 변경 디바운스 적용
- 대량 렌더링: 구조 표를 한 번에 그리지 않고 청크 단위로 채워 UI 프리징 완화

참고: UI/버튼 구성 등 UX는 유지하면서 내부 처리만 비동기/청크 처리로 변경했습니다.

## 라이선스
MIT License

---
문의: ohzzam (github.com/ohzzam)


## 화면

# 기본화면
<img width="1011" height="720" alt="image" src="https://github.com/user-attachments/assets/24f50cdb-1b74-4287-a938-87ed039af869" />

# 엑셀로 내보내기
<img width="1028" height="716" alt="image" src="https://github.com/user-attachments/assets/77b184a6-4d0c-4e0e-9669-a819d44a6c71" />
<img width="1019" height="721" alt="image" src="https://github.com/user-attachments/assets/8baf0a45-0c49-4335-88b4-f0d921ee6b70" />

# DDL 생성
<img width="1016" height="710" alt="image" src="https://github.com/user-attachments/assets/f3533f43-34b9-4711-aac5-217d5d7ceda2" />

# 엑셀 결과 
<img width="1276" height="418" alt="image" src="https://github.com/user-attachments/assets/b7b43ad0-55ab-45f7-9fb2-366b54f4f0be" />







