"""
MCP Context 시스템 테스트 및 데모

메타데이터 등록, 품질 규칙 실행, Context 패키지 생성 등의 테스트
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from hub_governance import (
    MetadataManager, TableMetadata, ColumnMetadata, DataClassification,
    QualityEngine, QualityRule, RuleSeverity,
    LineageTracker, LineageNode, LineageEdge, TransformationType,
    MCPContextBuilder
)
import json


def test_metadata_manager():
    """메타데이터 관리 테스트"""
    print("\n" + "="*80)
    print("1. 메타데이터 관리 테스트")
    print("="*80)
    
    mgr = MetadataManager()
    
    # 메타데이터 테이블 초기화
    print("\n✓ 메타데이터 테이블 생성 중...")
    mgr.init_metadata_tables()
    
    # 감사 로그 메타데이터 등록
    audit_log_metadata = TableMetadata(
        table_id="tbl_audit_001",
        table_name="agent_audit_log",
        database_name="test",
        description="에이전트 감사 로그 - 모든 쿼리 실행 이력 기록",
        owner="data_governance_team",
        owner_email="data@company.com",
        version="1.0.0",
        classification=DataClassification.INTERNAL,
        tags=["audit", "monitoring", "agent"],
        update_frequency="real-time",
        row_count=4,
        size_mb=0.05,
        columns=[
            ColumnMetadata(
                column_name="event_ts",
                data_type="DATETIME",
                nullable=False,
                description="이벤트 발생 시간",
                classification=DataClassification.INTERNAL,
                example_values=["2026-01-20 15:36:02"]
            ),
            ColumnMetadata(
                column_name="agent_id",
                data_type="VARCHAR(64)",
                nullable=False,
                description="에이전트 ID",
                classification=DataClassification.INTERNAL,
                example_values=["agri-data-agent-001"]
            ),
            ColumnMetadata(
                column_name="user_id",
                data_type="VARCHAR(64)",
                nullable=False,
                description="사용자 ID",
                classification=DataClassification.INTERNAL,
                example_values=["u0001", "test_user"]
            ),
            ColumnMetadata(
                column_name="query_template_id",
                data_type="VARCHAR(128)",
                nullable=False,
                description="쿼리 템플릿 ID",
                classification=DataClassification.INTERNAL,
                example_values=["Q001", "Q002"]
            ),
            ColumnMetadata(
                column_name="success",
                data_type="TINYINT",
                nullable=False,
                description="실행 성공 여부 (1=성공, 0=실패)",
                classification=DataClassification.INTERNAL,
                example_values=["1", "0"]
            )
        ]
    )
    
    print("\n✓ agent_audit_log 메타데이터 등록...")
    mgr.register_table_metadata(audit_log_metadata)
    
    # 등록된 메타데이터 조회
    print("\n✓ 등록된 메타데이터 조회...")
    metadata = mgr.get_table_metadata("tbl_audit_001")
    if metadata:
        print(f"  - 테이블명: {metadata.table_name}")
        print(f"  - 소유자: {metadata.owner}")
        print(f"  - 분류: {metadata.classification.value}")
        print(f"  - 컬럼 수: {len(metadata.columns)}")
        for col in metadata.columns[:3]:
            print(f"    • {col.column_name} ({col.data_type})")
    
    return audit_log_metadata


def test_quality_engine():
    """품질 엔진 테스트"""
    print("\n" + "="*80)
    print("2. 데이터 품질 규칙 테스트")
    print("="*80)
    
    engine = QualityEngine()
    
    # 품질 테이블 초기화
    print("\n✓ 품질 엔진 테이블 생성 중...")
    engine.init_quality_tables()
    
    # 품질 규칙 등록
    null_check_rule = QualityRule(
        rule_id="rule_001",
        rule_name="Null값 검사",
        rule_description="agent_audit_log의 완전성 검사",
        table_id="agent_audit_log",
        rule_type="null_check",
        condition_sql="SELECT COUNT(*) / COUNT(*) * 100 FROM agent_audit_log",
        threshold=95.0,
        severity=RuleSeverity.CRITICAL
    )
    
    print("\n✓ Null값 검사 규칙 등록...")
    engine.register_rule(null_check_rule)
    
    # 규칙 실행
    print("\n✓ Null값 검사 규칙 실행 중...")
    result = engine.execute_rule(null_check_rule)
    print(f"  - 규칙명: {result.rule_name}")
    print(f"  - 통과 여부: {'✓ 통과' if result.passed else '✗ 실패'}")
    print(f"  - 점수: {result.score:.2f}%")
    print(f"  - 임계값: {result.threshold}%")
    print(f"  - 메시지: {result.message}")
    
    # 유일성 검사 규칙 등록
    uniqueness_rule = QualityRule(
        rule_id="rule_002",
        rule_name="유일성 검사",
        rule_description="event_ts와 agent_id의 유일성 검사",
        table_id="agent_audit_log",
        column_name="event_ts",
        rule_type="uniqueness",
        condition_sql="SELECT COUNT(DISTINCT event_ts) / COUNT(*) * 100 FROM agent_audit_log",
        threshold=90.0,
        severity=RuleSeverity.WARNING
    )
    
    print("\n✓ 유일성 검사 규칙 등록...")
    engine.register_rule(uniqueness_rule)
    
    print("\n✓ 유일성 검사 규칙 실행 중...")
    result = engine.execute_rule(uniqueness_rule)
    print(f"  - 규칙명: {result.rule_name}")
    print(f"  - 통과 여부: {'✓ 통과' if result.passed else '✗ 실패'}")
    print(f"  - 점수: {result.score:.2f}%")
    
    return [null_check_rule, uniqueness_rule]


def test_lineage_tracker():
    """라인리지 추적 테스트"""
    print("\n" + "="*80)
    print("3. 데이터 라인리지 추적 테스트")
    print("="*80)
    
    tracker = LineageTracker()
    
    # 라인리지 테이블 초기화
    print("\n✓ 라인리지 추적 테이블 생성 중...")
    tracker.init_lineage_tables()
    
    # 데이터 소스 노드 추가
    print("\n✓ 데이터 소스 노드 추가 중...")
    source_node = LineageNode(
        node_id="node_source_001",
        node_name="원시 센서 데이터",
        node_type="source",
        database_name="raw",
        table_name="sensor_readings",
        description="농업 센서에서 수집한 원시 측정 데이터",
        owner="data_collection_team"
    )
    tracker.add_node(source_node)
    
    # 변환 노드 추가
    print("✓ 변환 처리 노드 추가 중...")
    transform_node = LineageNode(
        node_id="node_transform_001",
        node_name="정제된 센서 데이터",
        node_type="transformed",
        database_name="processed",
        table_name="sensor_readings_clean",
        description="이상치 제거 및 정규화된 센서 데이터",
        owner="data_processing_team"
    )
    tracker.add_node(transform_node)
    
    # 최종 테이블 노드 추가
    print("✓ 최종 데이터 노드 추가 중...")
    final_node = LineageNode(
        node_id="node_analytics_001",
        node_name="센서 분석 리포트",
        node_type="analytics",
        database_name="analytics",
        table_name="sensor_analytics",
        description="센서 데이터 기반 분석 리포트",
        owner="analytics_team"
    )
    tracker.add_node(final_node)
    
    # 엣지 추가 (변환 관계)
    print("\n✓ 데이터 변환 관계 추가 중...")
    edge1 = LineageEdge(
        edge_id="edge_001",
        source_node_id="node_source_001",
        target_node_id="node_transform_001",
        transformation_type=TransformationType.TRANSFORMATION,
        transformation_description="이상치 제거 및 정규화",
        job_id="job_etl_001"
    )
    tracker.add_edge(edge1)
    
    edge2 = LineageEdge(
        edge_id="edge_002",
        source_node_id="node_transform_001",
        target_node_id="node_analytics_001",
        transformation_type=TransformationType.AGGREGATION,
        transformation_description="시간별 집계 및 분석",
        job_id="job_analytics_001"
    )
    tracker.add_edge(edge2)
    
    print("\n✓ 라인리지 정보:")
    print(f"  - 데이터 소스: {source_node.table_name}")
    print(f"  - 변환 단계: {transform_node.table_name}")
    print(f"  - 최종 산출물: {final_node.table_name}")


def test_context_builder():
    """Context 빌더 테스트"""
    print("\n" + "="*80)
    print("4. MCP Context 패키지 생성 테스트")
    print("="*80)
    
    builder = MCPContextBuilder()
    
    # Context 패키지 생성
    print("\n✓ Context 패키지 생성 중...")
    context_pkg = builder.build_complete_context(
        package_id="ctx_pkg_001",
        package_name="Agent SQL Tool Context",
        table_ids=["tbl_audit_001"]
    )
    
    print(f"\n✓ Context 패키지 생성 완료")
    print(f"  - Package ID: {context_pkg.package_id}")
    print(f"  - 패키지명: {context_pkg.package_name}")
    print(f"  - 생성 시간: {context_pkg.generated_at}")
    print(f"  - 포함된 Context 계층: {len(context_pkg.context_metadata.get('context_layers', []))}개")
    
    # Context 내용 출력
    print(f"\n✓ 메타데이터 Context:")
    print(f"  - 총 자산: {context_pkg.metadata_context.get('total_assets', 0)}개")
    
    print(f"\n✓ 품질 Context:")
    print(f"  - 품질 상태: {context_pkg.quality_context.get('quality_status', 'UNKNOWN')}")
    print(f"  - 평균 점수: {context_pkg.quality_context.get('average_quality_score', 0)}%")
    
    print(f"\n✓ 거버넌스 Context:")
    print(f"  - 총 데이터 자산: {context_pkg.governance_context.get('total_assets', 0)}개")
    
    # JSON으로 내보내기
    print("\n✓ Context를 JSON으로 내보내는 중...")
    export_path = "c:\\Python\\MCP_LLM\\context_package.json"
    if builder.export_context_to_json(context_pkg, export_path):
        print(f"  - 저장 위치: {export_path}")
    
    # LLM 프롬프트 생성
    print("\n✓ LLM 프롬프트 생성 중...")
    llm_prompt = builder.export_context_for_llm_prompt(context_pkg)
    print("\n--- LLM 프롬프트 ---")
    print(llm_prompt)
    print("\n--- 프롬프트 끝 ---")
    
    return context_pkg


def main():
    """메인 테스트 함수"""
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█  MCP Context 시스템 - 데이터 거버넌스 플랫폼 테스트" + " "*25 + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
    
    try:
        # 1. 메타데이터 관리 테스트
        metadata = test_metadata_manager()
        
        # 2. 품질 엔진 테스트
        rules = test_quality_engine()
        
        # 3. 라인리지 추적 테스트
        test_lineage_tracker()
        
        # 4. Context 빌더 테스트
        context_pkg = test_context_builder()
        
        print("\n" + "█"*80)
        print("█" + " "*78 + "█")
        print("█  ✓ 모든 테스트 완료!" + " "*63 + "█")
        print("█" + " "*78 + "█")
        print("█"*80)
        
        print("\n다음 단계:")
        print("1. 추가 메타데이터 등록")
        print("2. 더 많은 품질 규칙 정의")
        print("3. 라인리지 맵 확장")
        print("4. LLM Agent와 Context 통합")
        print("5. 비정형 데이터 처리 시스템 구축")
        
    except Exception as e:
        print(f"\n✗ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
