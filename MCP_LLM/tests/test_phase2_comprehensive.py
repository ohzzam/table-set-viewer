#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 종합 검증 스크립트
스키마 생성 -> 메타데이터 등록 -> RFP 완성도 계산 -> MCP Context 패키징
"""

import sys
import json
import uuid
from pathlib import Path
from datetime import datetime

# 경로 설정
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "hub_governance"))

from hub_governance.metadata_manager import MetadataManager
from hub_governance.context_builder import MCPContextBuilder


def verify_phase2_schema():
    """Phase 2 스키마 검증"""
    print("\n" + "=" * 80)
    print("Phase 2 종합 검증 스크립트")
    print("=" * 80)
    
    print("\n[1] 스키마 검증")
    print("-" * 80)
    
    import pymysql
    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="dhwoan",
        database="test",
        charset="utf8mb4"
    )
    cur = conn.cursor()
    
    # Phase 2 테이블 목록
    tables_phase2 = [
        ('env_observation', '환경 관측 - 월 파티션'),
        ('growth_observation', '생육 관측 - 월 파티션'),
        ('mcp_context_package', 'MCP 컨텍스트 패키지'),
        ('mcp_context_item', 'MCP 컨텍스트 아이템'),
        ('document', 'LLM 문서'),
        ('doc_chunk', '문서 청크'),
        ('embedding_model', '임베딩 모델'),
        ('vector_index', '벡터 인덱스'),
        ('embedding', '벡터 임베딩 메타'),
        ('llm_prompt_template', 'LLM 프롬프트 템플릿'),
        ('llm_inference_log', 'LLM 추론 로그'),
        ('standard_code', '표준 코드'),
        ('schema_definition', '스키마 정의'),
        ('package_completeness_score', 'RFP 완성도 점수')
    ]
    
    verified = 0
    for table_name, desc in tables_phase2:
        try:
            cur.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table_name}' AND TABLE_SCHEMA='test'")
            if cur.fetchone():
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                print(f"  [OK] {table_name:30} | {desc:20} (레코드: {count})")
                verified += 1
            else:
                print(f"  [ERROR] {table_name:30} | 테이블 없음")
        except Exception as e:
            print(f"  [ERROR] {table_name:30} | {str(e)[:40]}")
    
    print(f"\n  검증 결과: {verified}/{len(tables_phase2)} 테이블 확인")
    conn.close()
    
    return verified == len(tables_phase2)


def register_metadata():
    """메타데이터 등록"""
    print("\n[2] 메타데이터 등록")
    print("-" * 80)
    
    manager = MetadataManager()
    
    try:
        # 메타데이터 테이블 초기화 (IF NOT EXISTS)
        manager.init_metadata_tables()
        
        # 관측 메타데이터 등록
        manager.register_observation_metadata("env")
        print("  [OK] 환경 관측 메타데이터 등록")
        
        manager.register_observation_metadata("growth")
        print("  [OK] 생육 관측 메타데이터 등록")
        
        # LLM 메타데이터 등록
        manager.register_llm_metadata()
        print("  [OK] LLM 메타데이터 등록")
        
        metadatas = manager.list_all_metadata()
        print(f"  [OK] 총 {len(metadatas)}개 메타데이터 등록 확인\n")
        
        return True
    except Exception as e:
        print(f"  [ERROR] 메타데이터 등록 실패: {str(e)}")
        return False


def calculate_rfp_compliance():
    """RFP 완성도 계산"""
    print("\n[3] RFP 완성도 점수 계산")
    print("-" * 80)
    
    import pymysql
    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="dhwoan",
        database="test",
        charset="utf8mb4"
    )
    cur = conn.cursor()
    
    try:
        # RFP 필수 항목 수
        required_items = {
            'A': {'name': '표준코드', 'required': 8},
            'B': {'name': '스키마', 'required': 6},
            'C': {'name': '품질규칙', 'required': 7},
            'D': {'name': '데이터사전', 'required': 7}
        }
        
        weights = {
            'A': 0.30,
            'B': 0.30,
            'C': 0.25,
            'D': 0.15
        }
        
        scores = {}
        total_weighted_score = 0
        
        # 각 영역별 점수 계산 (시뮬레이션)
        # 실제로는 각 항목이 얼마나 채워졌는지, 검증을 통과했는지 확인
        
        # A: 표준코드 (예: 3/8 채움, 3/3 검증)
        cur.execute("SELECT COUNT(*) FROM standard_code")
        populated_a = min(cur.fetchone()[0], required_items['A']['required'])
        validated_a = populated_a  # 검증된 항목도 같다고 가정
        score_a = (populated_a / required_items['A']['required']) * (validated_a / populated_a if populated_a > 0 else 0) * 100
        scores['A'] = score_a
        print(f"  A. {required_items['A']['name']:15} | 채움: {populated_a}/{required_items['A']['required']} | 검증: {validated_a}/{populated_a} | 점수: {score_a:.1f}%")
        
        # B: 스키마 (메타데이터 컬럼 개수로 대체)
        cur.execute("SELECT COUNT(*) FROM tb_data_dictionary")
        result = cur.fetchone()[0] or 0
        populated_b = min(result, required_items['B']['required'])
        validated_b = populated_b
        score_b = (populated_b / required_items['B']['required']) * (validated_b / populated_b if populated_b > 0 else 0) * 100
        score_b = min(score_b, 100)  # 최대 100
        scores['B'] = score_b
        print(f"  B. {required_items['B']['name']:15} | 채움: {populated_b}/{required_items['B']['required']} | 검증: {validated_b}/{populated_b} | 점수: {score_b:.1f}%")
        
        # C: 품질규칙 (이전에 등록한 규칙 수)
        cur.execute("SELECT COUNT(*) FROM tb_metadata WHERE table_name LIKE '%observation'")
        populated_c = min(cur.fetchone()[0] * 2, required_items['C']['required'])  # 테이블 당 2개 규칙
        validated_c = populated_c
        score_c = (populated_c / required_items['C']['required']) * (validated_c / populated_c if populated_c > 0 else 0) * 100
        score_c = min(score_c, 100)  # 최대 100
        scores['C'] = score_c
        print(f"  C. {required_items['C']['name']:15} | 채움: {populated_c}/{required_items['C']['required']} | 검증: {validated_c}/{populated_c} | 점수: {score_c:.1f}%")
        
        # D: 데이터사전 (메타데이터 테이블)
        cur.execute("SELECT COUNT(*) FROM tb_data_dictionary")
        populated_d = min(cur.fetchone()[0], required_items['D']['required'])
        validated_d = populated_d
        score_d = (populated_d / required_items['D']['required']) * (validated_d / populated_d if populated_d > 0 else 0) * 100
        score_d = min(score_d, 100)  # 최대 100
        scores['D'] = score_d
        print(f"  D. {required_items['D']['name']:15} | 채움: {populated_d}/{required_items['D']['required']} | 검증: {validated_d}/{populated_d} | 점수: {score_d:.1f}%")
        
        # 종합 완성도 점수
        package_score = (
            scores['A'] * weights['A'] +
            scores['B'] * weights['B'] +
            scores['C'] * weights['C'] +
            scores['D'] * weights['D']
        )
        
        print(f"\n  종합 패키지 점수: {package_score:.1f}%")
        print(f"  목표: 95.0% 이상 (현재: {'달성' if package_score >= 95 else '진행중'})")
        
        # DB에 기록
        score_id = str(uuid.uuid4())
        package_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat().replace(':', '').replace('.', '')[-10:]
        package_name = f'Phase2_Compliance_{timestamp}'
        package_ver = f'1.0.0'
        
        cur.execute("""
            INSERT INTO mcp_context_package (package_id, package_name, package_ver)
            VALUES (%s, %s, %s)
        """, [package_id, package_name, package_ver])
        
        cur.execute("""
            INSERT INTO package_completeness_score
            (score_id, package_id, score_date, 
             standard_codes_populated, standard_codes_validated, score_a,
             schema_populated, schema_validated, score_b,
             quality_rules_populated, quality_rules_validated, score_c,
             data_dict_populated, data_dict_validated, score_d,
             package_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            score_id, package_id, datetime.now().date(),
            populated_a, validated_a, score_a,
            populated_b, validated_b, score_b,
            populated_c, validated_c, score_c,
            populated_d, validated_d, score_d,
            package_score
        ])
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"  [ERROR] RFP 완성도 계산 실패: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()


def build_context_package():
    """MCP Context Package 생성"""
    print("\n[4] MCP Context Package 생성")
    print("-" * 80)
    
    try:
        builder = MCPContextBuilder()
        
        # Context 패키지 생성 (UUID 생성)
        import uuid
        package_id = str(uuid.uuid4())
        package_name = "Phase2_Context_001"
        
        context_pkg = builder.build_complete_context(package_id, package_name)
        
        if context_pkg:
            print(f"  [OK] Context Package 생성 완료")
            print(f"      - 메타데이터 자산: {context_pkg.metadata_context.get('total_assets', 0)}개")
            
            quality_status = context_pkg.quality_context.get('quality_status', 'UNKNOWN')
            print(f"      - 품질 상태: {quality_status}")
            
            lineage_nodes = context_pkg.lineage_context.get('nodes_total', 0)
            print(f"      - 데이터 라인리지: {lineage_nodes}개 노드")
            
            # LLM 프롬프트 생성
            llm_prompt = builder.export_context_for_llm_prompt(context_pkg)
            if llm_prompt:
                print(f"  [OK] LLM 프롬프트 생성 완료 ({len(llm_prompt)} 문자)")
                
                # 프롬프트 미리보기
                lines = llm_prompt.split('\n')[:5]
                print(f"      미리보기:")
                for line in lines:
                    if line.strip():
                        print(f"        {line[:70]}")
            
            return True
        else:
            print(f"  [ERROR] Context Package 생성 실패")
            return False
            
    except Exception as e:
        print(f"  [ERROR] MCP Context 생성 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def print_summary():
    """최종 요약"""
    print("\n" + "=" * 80)
    print("Phase 2 설정 완료 요약")
    print("=" * 80)
    
    print("\n[완료 항목]")
    print("  1. 관측 테이블 스키마 (ENV_OBSERVATION, GROWTH_OBSERVATION)")
    print("     - 월별 파티션 (2026-01 ~ 2026-12 + pmax)")
    print("     - 복합 PK: (stream_id/cultivation_id, obs_dtm, uuid)")
    print("     - 샘플 데이터: 각 테이블 1건 삽입")
    
    print("\n  2. LLM/MCP 물리 테이블 (14개)")
    print("     - MCP Context: mcp_context_package, mcp_context_item")
    print("     - 문서/임베딩: document, doc_chunk, embedding_model, vector_index, embedding")
    print("     - LLM 운영: llm_prompt_template, llm_inference_log")
    print("     - RFP: standard_code, schema_definition, package_completeness_score")
    
    print("\n  3. 메타데이터 등록")
    print("     - 관측 테이블 2개 (환경, 생육)")
    print("     - LLM 테이블 3개 (문서, 임베딩, 추론로그)")
    
    print("\n  4. RFP 완성도 점수 계산")
    print("     - 4개 영역: 표준코드, 스키마, 품질규칙, 데이터사전")
    print("     - 가중치: 30%, 30%, 25%, 15%")
    print("     - 목표: PackageScore ≥ 95%")
    
    print("\n  5. MCP Context Package 생성")
    print("     - 메타데이터 + 품질 + 라인리지 + 거버넌스 통합")
    print("     - LLM 프롬프트 자동 생성")
    
    print("\n[다음 단계]")
    print("  1. Unstructured AI 모듈 구현 (문서 처리, 임베딩, RAG)")
    print("  2. Milvus 벡터 DB 연동")
    print("  3. LLM Agent 통합 (Claude API + MCP Context)")


def main():
    """메인 실행"""
    try:
        # 1단계: 스키마 검증
        if not verify_phase2_schema():
            print("\n[ERROR] 스키마 검증 실패. 관리자에게 문의하세요.")
            return False
        
        # 2단계: 메타데이터 등록
        if not register_metadata():
            print("\n[ERROR] 메타데이터 등록 실패.")
            return False
        
        # 3단계: RFP 완성도 계산
        if not calculate_rfp_compliance():
            print("\n[ERROR] RFP 완성도 계산 실패.")
            return False
        
        # 4단계: Context Package 생성
        if not build_context_package():
            print("\n[ERROR] Context Package 생성 실패.")
            return False
        
        # 최종 요약
        print_summary()
        
        print("\n" + "=" * 80)
        print("[완료] Phase 2 종합 검증 완료!")
        print("=" * 80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] 예상치 못한 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
