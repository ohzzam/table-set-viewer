"""
데이터 라인리지 추적 모듈

데이터 출처, 변환 과정, 데이터 흐름 추적 (DAG)
"""

from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from enum import Enum
import json
import pymysql
from pydantic import BaseModel, Field


class TransformationType(str, Enum):
    """변환 유형"""
    EXTRACTION = "extraction"          # 추출
    TRANSFORMATION = "transformation"  # 변환
    LOADING = "loading"                # 로딩
    AGGREGATION = "aggregation"        # 집계
    JOIN = "join"                      # 조인
    FILTERING = "filtering"            # 필터링
    ENRICHMENT = "enrichment"          # 보강


class LineageNode(BaseModel):
    """라인리지 노드 (데이터 자산)"""
    node_id: str
    node_name: str
    node_type: str  # "table", "view", "dataset", "report"
    database_name: str
    table_name: Optional[str] = None
    description: Optional[str] = None
    owner: str
    created_date: datetime = Field(default_factory=datetime.now)


class LineageEdge(BaseModel):
    """라인리지 엣지 (변환 관계)"""
    edge_id: str
    source_node_id: str
    target_node_id: str
    transformation_type: TransformationType
    transformation_sql: Optional[str] = None
    transformation_description: Optional[str] = None
    job_id: Optional[str] = None
    executed_at: datetime = Field(default_factory=datetime.now)
    execution_duration_ms: int = 0


class LineageDAG(BaseModel):
    """데이터 라인리지 DAG"""
    dag_id: str
    dag_name: str
    description: Optional[str] = None
    nodes: Dict[str, LineageNode] = Field(default_factory=dict)
    edges: List[LineageEdge] = Field(default_factory=list)
    root_nodes: List[str] = Field(default_factory=list)  # 소스 데이터
    leaf_nodes: List[str] = Field(default_factory=list)  # 최종 데이터
    created_date: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)


# ============================================================================
# 라인리지 추적 엔진
# ============================================================================

class LineageTracker:
    """데이터 라인리지 추적 및 관리"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """
        라인리지 추적기 초기화
        
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
        self.lineage_graphs: Dict[str, LineageDAG] = {}
    
    def connect(self) -> pymysql.connections.Connection:
        """데이터베이스 연결"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def init_lineage_tables(self):
        """라인리지 관련 테이블 생성"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 라인리지 노드 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_lineage_node (
                    node_id VARCHAR(128) PRIMARY KEY,
                    node_name VARCHAR(256) NOT NULL,
                    node_type VARCHAR(100) NOT NULL,
                    database_name VARCHAR(128) NOT NULL,
                    table_name VARCHAR(256),
                    description TEXT,
                    owner VARCHAR(128) NOT NULL,
                    created_date DATETIME NOT NULL,
                    node_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_node_type (node_type),
                    INDEX idx_owner (owner)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # 라인리지 엣지 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_lineage_edge (
                    edge_id VARCHAR(128) PRIMARY KEY,
                    source_node_id VARCHAR(128) NOT NULL,
                    target_node_id VARCHAR(128) NOT NULL,
                    transformation_type VARCHAR(100) NOT NULL,
                    transformation_sql TEXT,
                    transformation_description TEXT,
                    job_id VARCHAR(128),
                    executed_at DATETIME NOT NULL,
                    execution_duration_ms INT,
                    edge_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (source_node_id) REFERENCES tb_lineage_node(node_id),
                    FOREIGN KEY (target_node_id) REFERENCES tb_lineage_node(node_id),
                    INDEX idx_source (source_node_id),
                    INDEX idx_target (target_node_id),
                    INDEX idx_transformation_type (transformation_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            # 라인리지 DAG 테이블
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tb_lineage_dag (
                    dag_id VARCHAR(128) PRIMARY KEY,
                    dag_name VARCHAR(256) NOT NULL,
                    description TEXT,
                    root_nodes JSON,
                    leaf_nodes JSON,
                    created_date DATETIME NOT NULL,
                    last_updated DATETIME NOT NULL,
                    dag_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_dag_name (dag_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            
            conn.commit()
            print("라인리지 추적 테이블 생성 완료")
            
        except Exception as e:
            conn.rollback()
            print(f"테이블 생성 오류: {e}")
        finally:
            conn.close()
    
    def add_node(self, node: LineageNode) -> bool:
        """라인리지 노드 추가"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tb_lineage_node (
                    node_id, node_name, node_type, database_name, table_name,
                    description, owner, created_date, node_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    node_name=VALUES(node_name),
                    description=VALUES(description)
            """, (
                node.node_id,
                node.node_name,
                node.node_type,
                node.database_name,
                node.table_name,
                node.description,
                node.owner,
                node.created_date,
                node.model_dump_json()
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"노드 추가 오류: {e}")
            return False
        finally:
            conn.close()
    
    def add_edge(self, edge: LineageEdge) -> bool:
        """라인리지 엣지 추가 (변환 관계)"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tb_lineage_edge (
                    edge_id, source_node_id, target_node_id, transformation_type,
                    transformation_sql, transformation_description, job_id,
                    executed_at, execution_duration_ms, edge_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                edge.edge_id,
                edge.source_node_id,
                edge.target_node_id,
                edge.transformation_type.value,
                edge.transformation_sql,
                edge.transformation_description,
                edge.job_id,
                edge.executed_at,
                edge.execution_duration_ms,
                edge.model_dump_json()
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"엣지 추가 오류: {e}")
            return False
        finally:
            conn.close()
    
    def build_dag(self, dag_id: str, dag_name: str, description: str = None) -> LineageDAG:
        """DAG 빌드"""
        dag = LineageDAG(
            dag_id=dag_id,
            dag_name=dag_name,
            description=description
        )
        return dag
    
    def save_dag(self, dag: LineageDAG) -> bool:
        """DAG 저장"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 루트 노드와 리프 노드 계산
            all_targets = set()
            all_sources = set()
            
            for edge in dag.edges:
                all_targets.add(edge.target_node_id)
                all_sources.add(edge.source_node_id)
            
            root_nodes = [n for n in all_sources if n not in all_targets]
            leaf_nodes = [n for n in all_targets if n not in all_sources]
            
            dag.root_nodes = root_nodes
            dag.leaf_nodes = leaf_nodes
            
            cur.execute("""
                INSERT INTO tb_lineage_dag (
                    dag_id, dag_name, description, root_nodes, leaf_nodes,
                    created_date, last_updated, dag_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    dag_name=VALUES(dag_name),
                    last_updated=VALUES(last_updated),
                    dag_json=VALUES(dag_json)
            """, (
                dag.dag_id,
                dag.dag_name,
                dag.description,
                json.dumps(dag.root_nodes),
                json.dumps(dag.leaf_nodes),
                dag.created_date,
                dag.last_updated,
                dag.model_dump_json(default=str)
            ))
            
            conn.commit()
            self.lineage_graphs[dag.dag_id] = dag
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"DAG 저장 오류: {e}")
            return False
        finally:
            conn.close()
    
    def get_upstream_nodes(self, node_id: str) -> List[LineageNode]:
        """상위 노드 조회 (데이터 출처)"""
        conn = self.connect()
        cur = conn.cursor()
        upstream = []
        
        try:
            visited = set()
            queue = [node_id]
            
            while queue:
                current_id = queue.pop(0)
                if current_id in visited:
                    continue
                visited.add(current_id)
                
                # 현재 노드의 소스 찾기
                cur.execute("""
                    SELECT DISTINCT source_node_id FROM tb_lineage_edge
                    WHERE target_node_id = %s
                """, (current_id,))
                
                sources = cur.fetchall()
                for source in sources:
                    source_id = source[0]
                    queue.append(source_id)
                    
                    # 노드 정보 조회
                    cur.execute("""
                        SELECT node_json FROM tb_lineage_node WHERE node_id = %s
                    """, (source_id,))
                    
                    node_result = cur.fetchone()
                    if node_result:
                        node_dict = json.loads(node_result[0])
                        upstream.append(LineageNode(**node_dict))
            
            return upstream
            
        except Exception as e:
            print(f"상위 노드 조회 오류: {e}")
            return []
        finally:
            conn.close()
    
    def get_downstream_nodes(self, node_id: str) -> List[LineageNode]:
        """하위 노드 조회 (데이터 영향도)"""
        conn = self.connect()
        cur = conn.cursor()
        downstream = []
        
        try:
            visited = set()
            queue = [node_id]
            
            while queue:
                current_id = queue.pop(0)
                if current_id in visited:
                    continue
                visited.add(current_id)
                
                # 현재 노드의 타겟 찾기
                cur.execute("""
                    SELECT DISTINCT target_node_id FROM tb_lineage_edge
                    WHERE source_node_id = %s
                """, (current_id,))
                
                targets = cur.fetchall()
                for target in targets:
                    target_id = target[0]
                    queue.append(target_id)
                    
                    # 노드 정보 조회
                    cur.execute("""
                        SELECT node_json FROM tb_lineage_node WHERE node_id = %s
                    """, (target_id,))
                    
                    node_result = cur.fetchone()
                    if node_result:
                        node_dict = json.loads(node_result[0])
                        downstream.append(LineageNode(**node_dict))
            
            return downstream
            
        except Exception as e:
            print(f"하위 노드 조회 오류: {e}")
            return []
        finally:
            conn.close()
    
    def get_transformation_path(self, source_node_id: str, target_node_id: str) -> List[LineageEdge]:
        """두 노드 사이의 변환 경로 조회"""
        conn = self.connect()
        cur = conn.cursor()
        paths = []
        
        try:
            # BFS로 경로 탐색
            visited = set()
            queue = [(source_node_id, [])]
            
            while queue:
                current_id, path = queue.pop(0)
                if current_id in visited:
                    continue
                visited.add(current_id)
                
                if current_id == target_node_id:
                    paths.append(path)
                    continue
                
                # 현재 노드에서 나가는 엣지 찾기
                cur.execute("""
                    SELECT edge_json FROM tb_lineage_edge
                    WHERE source_node_id = %s
                """, (current_id,))
                
                edges = cur.fetchall()
                for edge_result in edges:
                    edge_dict = json.loads(edge_result[0])
                    edge = LineageEdge(**edge_dict)
                    new_path = path + [edge]
                    queue.append((edge.target_node_id, new_path))
            
            return paths[0] if paths else []
            
        except Exception as e:
            print(f"변환 경로 조회 오류: {e}")
            return []
        finally:
            conn.close()
