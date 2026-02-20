#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schema Loader for Phase 2 - Observation + LLM/MCP Tables
관측 테이블 및 LLM/MCP 물리 스키마 생성 도구
"""

import json
import pymysql
import sys
from pathlib import Path
from datetime import datetime

# 프로젝트 경로
PROJECT_ROOT = Path(__file__).parent
CONFIG_DIR = PROJECT_ROOT / "config"


class SchemaLoader:
    def __init__(self, config_file):
        """Initialize schema loader with config"""
        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.db_config = self.config['database']
        self.observation_config = self.config.get('observation', {})
        self.rfp_config = self.config.get('rfp_compliance', {})
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to MariaDB"""
        try:
            self.conn = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset=self.db_config['charset']
            )
            self.cursor = self.conn.cursor()
            print(f"[OK] 데이터베이스 연결 성공: {self.db_config['database']}")
            return True
        except Exception as e:
            print(f"[ERROR] 데이터베이스 연결 실패: {str(e)}")
            return False

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("[OK] 데이터베이스 연결 종료")

    def load_schema_from_file(self, sql_file):
        """Load and execute SQL from file"""
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Split SQL statements (simple approach)
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                try:
                    self.cursor.execute(statement)
                    self.conn.commit()
                    print(f"[{i}] [OK] 스키마 생성 완료")
                except Exception as e:
                    self.conn.rollback()
                    print(f"[{i}] [ERROR] 스키마 생성 실패: {str(e)}")
            
            print(f"[OK] 총 {len(statements)}개 스키마 적용 완료")
            return True
        except Exception as e:
            print(f"[ERROR] 스키마 파일 로드 실패: {str(e)}")
            return False

    def verify_tables(self):
        """Verify created tables"""
        tables_to_check = [
            'env_observation',
            'growth_observation',
            'mcp_context_package',
            'mcp_context_item',
            'document',
            'doc_chunk',
            'embedding_model',
            'vector_index',
            'embedding',
            'llm_prompt_template',
            'llm_inference_log',
            'standard_code',
            'schema_definition',
            'package_completeness_score'
        ]
        
        verified = 0
        for table in tables_to_check:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                print(f"[OK] {table} (레코드: {count})")
                verified += 1
            except Exception as e:
                print(f"[ERROR] {table} 확인 실패: {str(e)}")
        
        print(f"\n[결과] {verified}/{len(tables_to_check)} 테이블 확인 완료")
        return verified == len(tables_to_check)

    def show_partition_info(self):
        """Show partition information for observation tables"""
        try:
            self.cursor.execute("""
                SELECT TABLE_NAME, PARTITION_NAME, PARTITION_METHOD
                FROM INFORMATION_SCHEMA.PARTITIONS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ('env_observation', 'growth_observation')
                ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION
            """, [self.db_config['database']])
            
            results = self.cursor.fetchall()
            if results:
                print("\n=== 파티션 정보 ===")
                for row in results:
                    print(f"테이블: {row[0]}, 파티션: {row[1]}")
                return True
            else:
                print("[INFO] 파티션 정보 없음")
                return False
        except Exception as e:
            print(f"[ERROR] 파티션 조회 실패: {str(e)}")
            return False

    def create_sample_data(self):
        """Create sample data for testing"""
        import uuid
        from datetime import datetime, timedelta
        
        try:
            # Sample environment observation
            env_obs_uuid = str(uuid.uuid4())
            stream_id = str(uuid.uuid4())
            obs_dtm = datetime(2026, 1, 15, 10, 30, 0)
            
            self.cursor.execute("""
                INSERT INTO env_observation 
                (env_obs_uuid, stream_id, obs_dtm, val_num, quality_flag, ingest_batch_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, [env_obs_uuid, stream_id, obs_dtm, 25.5, 'OK', str(uuid.uuid4())])
            
            # Sample growth observation
            growth_obs_uuid = str(uuid.uuid4())
            cultivation_id = str(uuid.uuid4())
            
            self.cursor.execute("""
                INSERT INTO growth_observation
                (growth_obs_uuid, cultivation_id, obs_dtm, indicator_type, val_num, quality_flag, ingest_batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [growth_obs_uuid, cultivation_id, obs_dtm, 'height', 30.2, 'OK', str(uuid.uuid4())])
            
            # Sample embedding model
            emb_model_id = str(uuid.uuid4())
            self.cursor.execute("""
                INSERT INTO embedding_model
                (emb_model_id, model_name, model_ver, dim)
                VALUES (%s, %s, %s, %s)
            """, [emb_model_id, 'sentence-transformers/multilingual-MiniLM-L12-v2', '1.0.0', 384])
            
            # Sample vector index
            index_id = str(uuid.uuid4())
            self.cursor.execute("""
                INSERT INTO vector_index
                (index_id, index_name, index_ver, backend_type)
                VALUES (%s, %s, %s, %s)
            """, [index_id, 'doc_embeddings_001', '1.0.0', 'milvus'])
            
            # Sample LLM prompt template
            template_id = str(uuid.uuid4())
            self.cursor.execute("""
                INSERT INTO llm_prompt_template
                (template_id, template_name, template_ver, purpose, prompt_ref)
                VALUES (%s, %s, %s, %s, %s)
            """, [template_id, 'data_labeling', '1.0.0', '자동 라벨링', 's3://prompts/labeling_v1.md'])
            
            # Sample MCP context package
            package_id = str(uuid.uuid4())
            self.cursor.execute("""
                INSERT INTO mcp_context_package
                (package_id, package_name, package_ver)
                VALUES (%s, %s, %s)
            """, [package_id, 'Phase2_Release', '1.0.0'])
            
            self.conn.commit()
            print("[OK] 샘플 데이터 삽입 완료")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] 샘플 데이터 삽입 실패: {str(e)}")
            return False


def main():
    config_file = CONFIG_DIR / "governance_config.json"
    schema_file = CONFIG_DIR / "schema_phase2.sql"
    
    print("=" * 70)
    print("Phase 2 스키마 로더 - 관측 테이블 + LLM/MCP 물리 테이블")
    print("=" * 70)
    
    loader = SchemaLoader(config_file)
    
    if not loader.connect():
        return False
    
    try:
        # 스키마 파일 로드
        print("\n[1단계] 스키마 파일 적용...")
        if not loader.load_schema_from_file(schema_file):
            return False
        
        # 테이블 검증
        print("\n[2단계] 테이블 검증...")
        if not loader.verify_tables():
            return False
        
        # 파티션 정보 확인
        print("\n[3단계] 파티션 정보 확인...")
        loader.show_partition_info()
        
        # 샘플 데이터 생성
        print("\n[4단계] 샘플 데이터 생성...")
        loader.create_sample_data()
        
        print("\n" + "=" * 70)
        print("[완료] Phase 2 스키마 설정 완료!")
        print("=" * 70)
        return True
    
    finally:
        loader.close()


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
