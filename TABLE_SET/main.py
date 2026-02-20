# DB Table Viewer - 확장/개선 포인트 포함
# 주요 기능:
# - 다양한 DB 지원 (MySQL, MariaDB, PostgreSQL)
# - 테이블 구조 복수 선택/엑셀 저장
# - DDL 복사/저장/미리보기
# - 접속 이력 관리
#
# 실행: python main.py

from gui_main import MainWindow
from PyQt5.QtWidgets import QApplication
import sys

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
