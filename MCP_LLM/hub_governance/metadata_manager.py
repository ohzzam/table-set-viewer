"""
데이터 거버넌스 플랫폼 - 메타데이터 관리 모듈

메타데이터 모델, CRUD 작업, 데이터사전 관리 등을 담당합니다.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
import json
import pymysql
from pydantic import BaseModel, Field


# ============================================================================
# 1. 열거형 정의
# ============================================================================

class DataClassification(str, Enum):
    """데이터 분류 등급"""
    PUBLIC = "public"           # 공개
    INTERNAL = "internal"       # 내부
    CONFIDENTIAL = "confidential"  # 기밀
    RESTRICTED = "restricted"   # 제한


class DataQualityDimension(str, Enum):
    """데이터 품질 차원"""
    COMPLETENESS = "completeness"    # 완전성 (null 없음)
    ACCURACY = "accuracy"            # 정확성 (정확한 값)
    CONSISTENCY = "consistency"      # 일관성 (포맷 일관)
    TIMELINESS = "timeliness"        # 적시성 (최신 여부)
    UNIQUENESS = "uniqueness"        # 유일성 (중복 없음)


# ============================================================================
# 2. Pydantic 모델 정의 (메타데이터 스키마)
# ============================================================================

class ColumnMetadata(BaseModel):
    """컬럼 레벨 메타데이터"""
    column_name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    classification: DataClassification = DataClassification.INTERNAL
    regex_pattern: Optional[str] = None  # 유효성 검사용
    example_values: List[str] = Field(default_factory=list)


class TableMetadata(BaseModel):
    """테이블 레벨 메타데이터"""
    table_id: str
    table_name: str
    database_name: str
    description: Optional[str] = None
    owner: str
    owner_email: Optional[str] = None
    created_date: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    version: str = "1.0.0"
    classification: DataClassification = DataClassification.INTERNAL
    tags: List[str] = Field(default_factory=list)
    columns: List[ColumnMetadata] = Field(default_factory=list)
    row_count: int = 0
    size_mb: float = 0.0
    update_frequency: str = "daily"  # daily, weekly, monthly, on-demand
    lineage: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_id": "tbl_001",
                "table_name": "agent_audit_log",
                "database_name": "test",
                "description": "에이전트 감사 로그",
                "owner": "data_team",
                "version": "1.0.0"
            }
        }


class DataQualityScore(BaseModel):
    """데이터 품질 점수"""
    table_id: str
    measured_at: datetime = Field(default_factory=datetime.now)
    completeness_score: float = Field(ge=0, le=100)
    accuracy_score: float = Field(ge=0, le=100)
    consistency_score: float = Field(ge=0, le=100)
    timeliness_score: float = Field(ge=0, le=100)
    uniqueness_score: float = Field(ge=0, le=100)
    overall_score: float = Field(default=0, ge=0, le=100)
    issues: List[str] = Field(default_factory=list)
    
    def calculate_overall(self, weights: Dict[str, float] = None):
        """가중 평균으로 전체 점수 계산"""
        if weights is None:
            weights = {
                "completeness": 0.2,
                "accuracy": 0.3,
                "consistency": 0.2,
                "timeliness": 0.2,
                "uniqueness": 0.1
            }
        
        self.overall_score = (
            self.completeness_score * weights.get("completeness", 0.2) +
            self.accuracy_score * weights.get("accuracy", 0.3) +
            self.consistency_score * weights.get("consistency", 0.2) +
            self.timeliness_score * weights.get("timeliness", 0.2) +
            self.uniqueness_score * weights.get("uniqueness", 0.1)
        )
        return self.overall_score


# ============================================================================
# 3. 메타데이터 관리자 클래스
# ============================================================================

class MetadataManager:
    """메타데이터 저장소 및 관리 도구"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root", 
                 password: str = "dhwoan", database: str = "test"):
        """
        메타데이터 관리자 초기화
        
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
        self.metadata_store: Dict[str, TableMetadata] = {}
        self.quality_scores: Dict[str, List[DataQualityScore]] = {}
        
    def connect(self) -> pymysql.connections.Connection:
        """데이터베이스 연결"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def init_metadata_tables(self):
        """메타데이터 저장용 테이블 생성"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 메타데이터 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_metadata (
                    metadata_id INT AUTO_INCREMENT PRIMARY KEY,
                    table_id VARCHAR(128) UNIQUE NOT NULL,
                    table_name VARCHAR(256) NOT NULL,
                    database_name VARCHAR(128) NOT NULL,
                    description TEXT,
                    owner VARCHAR(128) NOT NULL,
                    owner_email VARCHAR(256),
                    created_date DATETIME NOT NULL,
                    last_modified DATETIME NOT NULL,
                    version VARCHAR(20) NOT NULL,
                    classification VARCHAR(50) NOT NULL,
                    tags JSON,
                    row_count INT DEFAULT 0,
                    size_mb DECIMAL(10,2) DEFAULT 0,
                    update_frequency VARCHAR(50),
                    lineage JSON,
                    metadata_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_table_id (table_id),
                    INDEX idx_owner (owner),
                    INDEX idx_classification (classification)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # 품질 점수 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_quality_score (
                    quality_score_id INT AUTO_INCREMENT PRIMARY KEY,
                    table_id VARCHAR(128) NOT NULL,
                    measured_at DATETIME NOT NULL,
                    completeness_score FLOAT NOT NULL,
                    accuracy_score FLOAT NOT NULL,
                    consistency_score FLOAT NOT NULL,
                    timeliness_score FLOAT NOT NULL,
                    uniqueness_score FLOAT NOT NULL,
                    overall_score FLOAT NOT NULL,
                    issues JSON,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (table_id) REFERENCES tb_metadata(table_id),
                    INDEX idx_table_id (table_id),
                    INDEX idx_measured_at (measured_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # 데이터사전 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_data_dictionary (
                    dictionary_id INT AUTO_INCREMENT PRIMARY KEY,
                    table_id VARCHAR(128) NOT NULL,
                    column_name VARCHAR(256) NOT NULL,
                    data_type VARCHAR(100) NOT NULL,
                    nullable BOOLEAN DEFAULT TRUE,
                    description TEXT,
                    classification VARCHAR(50) NOT NULL,
                    regex_pattern VARCHAR(500),
                    example_values JSON,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (table_id) REFERENCES tb_metadata(table_id),
                    INDEX idx_table_id (table_id),
                    UNIQUE KEY unique_column (table_id, column_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            print("메타데이터 테이블 생성 완료")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"테이블 생성 오류: {e}")
            return False
        finally:
            conn.close()
    
    def register_table_metadata(self, metadata: TableMetadata) -> bool:
        """테이블 메타데이터 등록"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 메타데이터 저장
            cur.execute("""
                INSERT INTO tb_metadata (
                    table_id, table_name, database_name, description, owner, owner_email,
                    created_date, last_modified, version, classification, tags, 
                    row_count, size_mb, update_frequency, lineage, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    last_modified=VALUES(last_modified),
                    version=VALUES(version),
                    metadata_json=VALUES(metadata_json)
            """, (
                metadata.table_id,
                metadata.table_name,
                metadata.database_name,
                metadata.description,
                metadata.owner,
                metadata.owner_email,
                metadata.created_date,
                metadata.last_modified,
                metadata.version,
                metadata.classification.value,
                json.dumps(metadata.tags),
                metadata.row_count,
                metadata.size_mb,
                metadata.update_frequency,
                json.dumps(metadata.lineage),
                metadata.model_dump_json()
            ))
            
            # 컬럼 메타데이터 저장
            for column in metadata.columns:
                cur.execute("""
                    INSERT INTO tb_data_dictionary (
                        table_id, column_name, data_type, nullable, description,
                        classification, regex_pattern, example_values
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        data_type=VALUES(data_type),
                        nullable=VALUES(nullable),
                        description=VALUES(description),
                        classification=VALUES(classification),
                        regex_pattern=VALUES(regex_pattern),
                        example_values=VALUES(example_values)
                """, (
                    metadata.table_id,
                    column.column_name,
                    column.data_type,
                    column.nullable,
                    column.description,
                    column.classification.value,
                    column.regex_pattern,
                    json.dumps(column.example_values)
                ))
            
            conn.commit()
            self.metadata_store[metadata.table_id] = metadata
            print(f"메타데이터 등록 완료: {metadata.table_id}")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"메타데이터 등록 오류: {e}")
            return False
        finally:
            conn.close()
    
    def get_table_metadata(self, table_id: str) -> Optional[TableMetadata]:
        """테이블 메타데이터 조회"""
        if table_id in self.metadata_store:
            return self.metadata_store[table_id]
        
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT metadata_json FROM tb_metadata WHERE table_id = %s", (table_id,))
            result = cur.fetchone()
            
            if result:
                metadata_dict = json.loads(result[0])
                # 데이터사전 로드
                cur.execute("""
                    SELECT column_name, data_type, nullable, description, classification, 
                           regex_pattern, example_values
                    FROM tb_data_dictionary WHERE table_id = %s
                """, (table_id,))
                
                columns = []
                for col in cur.fetchall():
                    columns.append(ColumnMetadata(
                        column_name=col[0],
                        data_type=col[1],
                        nullable=col[2],
                        description=col[3],
                        classification=col[4],
                        regex_pattern=col[5],
                        example_values=json.loads(col[6]) if col[6] else []
                    ))
                
                metadata_dict['columns'] = columns
                metadata = TableMetadata(**metadata_dict)
                self.metadata_store[table_id] = metadata
                return metadata
            
            return None
            
        except Exception as e:
            print(f"메타데이터 조회 오류: {e}")
            return None
        finally:
            conn.close()
    
    def record_quality_score(self, score: DataQualityScore) -> bool:
        """품질 점수 기록"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tb_quality_score (
                    table_id, measured_at, completeness_score, accuracy_score,
                    consistency_score, timeliness_score, uniqueness_score, 
                    overall_score, issues
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                score.table_id,
                score.measured_at,
                score.completeness_score,
                score.accuracy_score,
                score.consistency_score,
                score.timeliness_score,
                score.uniqueness_score,
                score.overall_score,
                json.dumps(score.issues)
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"품질 점수 기록 오류: {e}")
            return False
        finally:
            conn.close()
    
    def list_all_metadata(self) -> List[TableMetadata]:
        """모든 메타데이터 목록 조회"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT table_id FROM tb_metadata")
            table_ids = [row[0] for row in cur.fetchall()]
            
            metadatas = []
            for table_id in table_ids:
                metadata = self.get_table_metadata(table_id)
                if metadata:
                    metadatas.append(metadata)
            
            return metadatas
            
        except Exception as e:
            print(f"메타데이터 목록 조회 오류: {e}")
            return []
        finally:
            conn.close()
    
    def register_observation_metadata(self, table_type: str = "env") -> bool:
        """
        관측 테이블 메타데이터 등록
        
        Args:
            table_type: "env" (환경관측) 또는 "growth" (생육관측)
        """
        import uuid
        
        if table_type == "env":
            metadata = TableMetadata(
                table_id=str(uuid.uuid4()),
                table_name="env_observation",
                database_name=self.database,
                description="환경 센서 데이터 - 월별 파티션",
                owner="data_team",
                owner_email="data-team@example.com",
                version="1.0.0",
                classification=DataClassification.INTERNAL,
                tags=["sensor", "real-time", "partitioned"],
                columns=[
                    ColumnMetadata(
                        column_name="env_obs_uuid",
                        data_type="CHAR(36)",
                        nullable=False,
                        description="환경 관측 UUID"
                    ),
                    ColumnMetadata(
                        column_name="stream_id",
                        data_type="CHAR(36)",
                        nullable=False,
                        description="센서 스트림 ID"
                    ),
                    ColumnMetadata(
                        column_name="obs_dtm",
                        data_type="DATETIME",
                        nullable=False,
                        description="관측 시간 (파티션 키)"
                    ),
                    ColumnMetadata(
                        column_name="val_num",
                        data_type="DECIMAL(18,6)",
                        nullable=False,
                        description="관측 수치값"
                    ),
                    ColumnMetadata(
                        column_name="quality_flag",
                        data_type="VARCHAR(20)",
                        nullable=True,
                        description="품질 플래그 (OK/WARNING/ERROR)"
                    )
                ],
                update_frequency="real-time"
            )
        else:  # growth
            metadata = TableMetadata(
                table_id=str(uuid.uuid4()),
                table_name="growth_observation",
                database_name=self.database,
                description="생육 관측 데이터 - 월별 파티션",
                owner="data_team",
                owner_email="data-team@example.com",
                version="1.0.0",
                classification=DataClassification.INTERNAL,
                tags=["cultivation", "indicators", "partitioned"],
                columns=[
                    ColumnMetadata(
                        column_name="growth_obs_uuid",
                        data_type="CHAR(36)",
                        nullable=False,
                        description="생육 관측 UUID"
                    ),
                    ColumnMetadata(
                        column_name="cultivation_id",
                        data_type="CHAR(36)",
                        nullable=False,
                        description="재배 ID"
                    ),
                    ColumnMetadata(
                        column_name="obs_dtm",
                        data_type="DATETIME",
                        nullable=False,
                        description="관측 시간 (파티션 키)"
                    ),
                    ColumnMetadata(
                        column_name="indicator_type",
                        data_type="VARCHAR(50)",
                        nullable=False,
                        description="지표 유형 (height/leaf_count 등)"
                    ),
                    ColumnMetadata(
                        column_name="val_num",
                        data_type="DECIMAL(18,6)",
                        nullable=True,
                        description="지표값"
                    )
                ],
                update_frequency="daily"
            )
        
        return self.register_table_metadata(metadata)
    
    def register_llm_metadata(self) -> bool:
        """LLM/문서 처리 관련 메타데이터 등록"""
        import uuid
        
        tables_to_register = [
            TableMetadata(
                table_id=str(uuid.uuid4()),
                table_name="document",
                database_name=self.database,
                description="LLM 학습 및 검색용 문서 저장소",
                owner="ai_team",
                version="1.0.0",
                classification=DataClassification.INTERNAL,
                tags=["llm", "knowledge", "unstructured"],
                columns=[
                    ColumnMetadata(column_name="doc_id", data_type="CHAR(36)", nullable=False),
                    ColumnMetadata(column_name="doc_type", data_type="VARCHAR(50)", nullable=False),
                    ColumnMetadata(column_name="title", data_type="VARCHAR(300)", nullable=True)
                ]
            ),
            TableMetadata(
                table_id=str(uuid.uuid4()),
                table_name="embedding",
                database_name=self.database,
                description="벡터 임베딩 메타데이터 (Milvus 인덱스 참조)",
                owner="ai_team",
                version="1.0.0",
                classification=DataClassification.INTERNAL,
                tags=["vector", "embedding", "rag"],
                columns=[
                    ColumnMetadata(column_name="emb_id", data_type="CHAR(36)", nullable=False),
                    ColumnMetadata(column_name="chunk_id", data_type="CHAR(36)", nullable=False),
                    ColumnMetadata(column_name="vector_ref", data_type="VARCHAR(1000)", nullable=False, 
                                 description="Milvus 벡터 ID 참조")
                ]
            ),
            TableMetadata(
                table_id=str(uuid.uuid4()),
                table_name="llm_inference_log",
                database_name=self.database,
                description="LLM 호출 이력 및 성능 로그 (감사·재현성)",
                owner="ai_team",
                version="1.0.0",
                classification=DataClassification.INTERNAL,
                tags=["llm", "audit", "performance"],
                columns=[
                    ColumnMetadata(column_name="infer_id", data_type="CHAR(36)", nullable=False),
                    ColumnMetadata(column_name="template_id", data_type="CHAR(36)", nullable=False),
                    ColumnMetadata(column_name="status", data_type="VARCHAR(30)", nullable=False),
                    ColumnMetadata(column_name="latency_ms", data_type="INT", nullable=True)
                ]
            )
        ]
        
        success = True
        for metadata in tables_to_register:
            if not self.register_table_metadata(metadata):
                success = False
        
        return success
