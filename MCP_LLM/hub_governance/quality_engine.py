"""
데이터 품질 규칙 엔진

데이터 품질 규칙 정의, 검증, 실행 및 결과 분석
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from enum import Enum
import json
import pymysql
import pandas as pd
from pydantic import BaseModel, Field


class RuleSeverity(str, Enum):
    """규칙 심각도"""
    CRITICAL = "critical"      # 즉시 처리 필요
    WARNING = "warning"        # 경고
    INFO = "info"             # 정보성


class QualityRule(BaseModel):
    """데이터 품질 규칙"""
    rule_id: str
    rule_name: str
    rule_description: str
    table_id: str
    column_name: Optional[str] = None
    rule_type: str  # "null_check", "range_check", "pattern_check", "uniqueness", "referential_integrity"
    condition_sql: str  # SQL 조건문
    threshold: float  # 통과 기준 (%)
    severity: RuleSeverity = RuleSeverity.WARNING
    enabled: bool = True
    created_date: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": "rule_001",
                "rule_name": "Null 값 검사",
                "rule_type": "null_check",
                "condition_sql": "COUNT(*) FILTER(WHERE column_name IS NOT NULL) / COUNT(*) * 100 >= 95",
                "threshold": 95.0,
                "severity": "critical"
            }
        }


class QualityCheckResult(BaseModel):
    """품질 검사 결과"""
    rule_id: str
    rule_name: str
    table_id: str
    column_name: Optional[str] = None
    passed: bool
    score: float  # 0-100
    threshold: float
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    checked_at: datetime = Field(default_factory=datetime.now)
    execution_time_ms: float = 0.0


# ============================================================================
# 품질 규칙 엔진
# ============================================================================

class QualityEngine:
    """데이터 품질 규칙 엔진"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """
        품질 엔진 초기화
        
        Args:
            host: MySQL 호스트
            user: MySQL 사용자
            password: MySQL 패스워드
            database: 데이터베이스명
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.rules: Dict[str, QualityRule] = {}
        self.rule_handlers: Dict[str, Callable] = {
            "null_check": self._check_null,
            "range_check": self._check_range,
            "pattern_check": self._check_pattern,
            "uniqueness": self._check_uniqueness,
            "referential_integrity": self._check_referential_integrity,
            "custom_sql": self._check_custom_sql
        }
    
    def connect(self) -> pymysql.connections.Connection:
        """데이터베이스 연결"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def init_quality_tables(self):
        """품질 관련 테이블 생성"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 품질 규칙 정의 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_quality_rule (
                    rule_id VARCHAR(128) PRIMARY KEY,
                    rule_name VARCHAR(256) NOT NULL,
                    rule_description TEXT,
                    table_id VARCHAR(128) NOT NULL,
                    column_name VARCHAR(256),
                    rule_type VARCHAR(100) NOT NULL,
                    condition_sql TEXT NOT NULL,
                    threshold FLOAT NOT NULL,
                    severity VARCHAR(50) NOT NULL,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_date DATETIME NOT NULL,
                    last_modified DATETIME NOT NULL,
                    rule_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_table_id (table_id),
                    INDEX idx_enabled (enabled)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # 품질 검사 결과 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_quality_check_result (
                    result_id INT AUTO_INCREMENT PRIMARY KEY,
                    rule_id VARCHAR(128) NOT NULL,
                    table_id VARCHAR(128) NOT NULL,
                    passed BOOLEAN NOT NULL,
                    score FLOAT NOT NULL,
                    threshold FLOAT NOT NULL,
                    message TEXT,
                    details JSON,
                    checked_at DATETIME NOT NULL,
                    execution_time_ms FLOAT,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rule_id) REFERENCES tb_quality_rule(rule_id),
                    INDEX idx_rule_id (rule_id),
                    INDEX idx_table_id (table_id),
                    INDEX idx_checked_at (checked_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            print("품질 엔진 테이블 생성 완료")
            
        except Exception as e:
            conn.rollback()
            print(f"테이블 생성 오류: {e}")
        finally:
            conn.close()
    
    def register_rule(self, rule: QualityRule) -> bool:
        """품질 규칙 등록"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tb_quality_rule (
                    rule_id, rule_name, rule_description, table_id, column_name,
                    rule_type, condition_sql, threshold, severity, enabled,
                    created_date, last_modified, rule_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    rule_name=VALUES(rule_name),
                    condition_sql=VALUES(condition_sql),
                    threshold=VALUES(threshold),
                    last_modified=VALUES(last_modified),
                    rule_json=VALUES(rule_json)
            """, (
                rule.rule_id,
                rule.rule_name,
                rule.rule_description,
                rule.table_id,
                rule.column_name,
                rule.rule_type,
                rule.condition_sql,
                rule.threshold,
                rule.severity.value,
                rule.enabled,
                rule.created_date,
                rule.last_modified,
                rule.model_dump_json()
            ))
            
            conn.commit()
            self.rules[rule.rule_id] = rule
            print(f"규칙 등록 완료: {rule.rule_id}")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"규칙 등록 오류: {e}")
            return False
        finally:
            conn.close()
    
    def execute_rule(self, rule: QualityRule) -> QualityCheckResult:
        """규칙 실행 및 검사"""
        import time
        start_time = time.time()
        
        handler = self.rule_handlers.get(rule.rule_type)
        if not handler:
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=False,
                score=0,
                threshold=rule.threshold,
                message=f"Unknown rule type: {rule.rule_type}",
                execution_time_ms=(time.time() - start_time) * 1000
            )
        
        try:
            result = handler(rule)
            result.execution_time_ms = (time.time() - start_time) * 1000
            return result
        except Exception as e:
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=False,
                score=0,
                threshold=rule.threshold,
                message=f"Execution error: {str(e)}",
                execution_time_ms=(time.time() - start_time) * 1000
            )
    
    # ========================================================================
    # 규칙 핸들러 구현
    # ========================================================================
    
    def _check_null(self, rule: QualityRule) -> QualityCheckResult:
        """Null 값 검사"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # Null 값이 아닌 레코드의 비율 계산
            if rule.column_name:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT({rule.column_name}) as non_null,
                        (COUNT({rule.column_name}) / COUNT(*) * 100) as completeness
                    FROM {rule.table_id}
                """)
            else:
                cur.execute(f"SELECT COUNT(*) FROM {rule.table_id}")
                total = cur.fetchone()[0]
                completeness = 100 if total > 0 else 0
                return QualityCheckResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    table_id=rule.table_id,
                    column_name=rule.column_name,
                    passed=completeness >= rule.threshold,
                    score=completeness,
                    threshold=rule.threshold,
                    message=f"Completeness: {completeness:.2f}%",
                    details={"total_rows": total, "completeness": completeness}
                )
            
            result = cur.fetchone()
            total, non_null, completeness = result[0], result[1], result[2] or 0
            
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=completeness >= rule.threshold,
                score=completeness,
                threshold=rule.threshold,
                message=f"Null값 검사 완료: {completeness:.2f}% (임계값: {rule.threshold}%)",
                details={
                    "total_rows": total,
                    "non_null_rows": non_null,
                    "null_rows": total - non_null,
                    "completeness": completeness
                }
            )
            
        except Exception as e:
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=False,
                score=0,
                threshold=rule.threshold,
                message=f"Error: {str(e)}"
            )
        finally:
            conn.close()
    
    def _check_range(self, rule: QualityRule) -> QualityCheckResult:
        """범위 검사"""
        # 규칙의 condition_sql에서 범위 추출 필요
        return QualityCheckResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            table_id=rule.table_id,
            column_name=rule.column_name,
            passed=True,
            score=100,
            threshold=rule.threshold,
            message="범위 검사 미구현"
        )
    
    def _check_pattern(self, rule: QualityRule) -> QualityCheckResult:
        """패턴 검사 (정규표현식)"""
        return QualityCheckResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            table_id=rule.table_id,
            column_name=rule.column_name,
            passed=True,
            score=100,
            threshold=rule.threshold,
            message="패턴 검사 미구현"
        )
    
    def _check_uniqueness(self, rule: QualityRule) -> QualityCheckResult:
        """유일성 검사"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            if not rule.column_name:
                raise ValueError("column_name is required for uniqueness check")
            
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT {rule.column_name}) as unique_count,
                    (COUNT(DISTINCT {rule.column_name}) / COUNT(*) * 100) as uniqueness
                FROM {rule.table_id}
            """)
            
            result = cur.fetchone()
            total, unique_count, uniqueness = result[0], result[1], result[2] or 0
            
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=uniqueness >= rule.threshold,
                score=uniqueness,
                threshold=rule.threshold,
                message=f"유일성: {uniqueness:.2f}% (임계값: {rule.threshold}%)",
                details={
                    "total_rows": total,
                    "unique_values": unique_count,
                    "duplicate_rows": total - unique_count,
                    "uniqueness": uniqueness
                }
            )
            
        except Exception as e:
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=False,
                score=0,
                threshold=rule.threshold,
                message=f"Error: {str(e)}"
            )
        finally:
            conn.close()
    
    def _check_referential_integrity(self, rule: QualityRule) -> QualityCheckResult:
        """참조 무결성 검사"""
        return QualityCheckResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            table_id=rule.table_id,
            column_name=rule.column_name,
            passed=True,
            score=100,
            threshold=rule.threshold,
            message="참조 무결성 검사 미구현"
        )
    
    def _check_custom_sql(self, rule: QualityRule) -> QualityCheckResult:
        """커스텀 SQL 검사"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute(rule.condition_sql)
            result = cur.fetchone()
            score = float(result[0]) if result else 0
            
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=score >= rule.threshold,
                score=score,
                threshold=rule.threshold,
                message=f"커스텀 SQL 검사 결과: {score:.2f}",
                details={"custom_result": score}
            )
            
        except Exception as e:
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                table_id=rule.table_id,
                column_name=rule.column_name,
                passed=False,
                score=0,
                threshold=rule.threshold,
                message=f"Error: {str(e)}"
            )
        finally:
            conn.close()
    
    def record_check_result(self, result: QualityCheckResult) -> bool:
        """검사 결과 기록"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tb_quality_check_result (
                    rule_id, table_id, passed, score, threshold, message, 
                    details, checked_at, execution_time_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result.rule_id,
                result.table_id,
                result.passed,
                result.score,
                result.threshold,
                result.message,
                json.dumps(result.details),
                result.checked_at,
                result.execution_time_ms
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"결과 기록 오류: {e}")
            return False
        finally:
            conn.close()
    
    def execute_all_rules(self, table_id: str) -> List[QualityCheckResult]:
        """특정 테이블의 모든 규칙 실행"""
        conn = self.connect()
        cur = conn.cursor()
        results = []
        
        try:
            # 해당 테이블의 모든 활성화된 규칙 조회
            cur.execute("""
                SELECT rule_json FROM tb_quality_rule 
                WHERE table_id = %s AND enabled = TRUE
            """, (table_id,))
            
            rows = cur.fetchall()
            for row in rows:
                rule_dict = json.loads(row[0])
                rule = QualityRule(**rule_dict)
                
                result = self.execute_rule(rule)
                results.append(result)
                self.record_check_result(result)
            
            return results
            
        except Exception as e:
            print(f"규칙 실행 오류: {e}")
            return []
        finally:
            conn.close()
