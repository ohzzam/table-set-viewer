#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 메타데이터 등록 테스트
관측 테이블 + LLM 메타데이터를 거버넌스 시스템에 등록
"""

import sys
from pathlib import Path

# 프로젝트 경로 설정
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "hub_governance"))

from hub_governance.metadata_manager import MetadataManager, DataClassification


def test_observation_metadata():
    """관측 테이블 메타데이터 등록 테스트"""
    print("\n" + "=" * 70)
    print("Phase 2 메타데이터 등록 - 관측 테이블 + LLM")
    print("=" * 70)
    
    manager = MetadataManager(
        host="127.0.0.1",
        user="root",
        password="dhwoan",
        database="test"
    )
    
    try:
        # 1단계: 메타데이터 테이블 초기화
        print("\n[1단계] 메타데이터 관리 테이블 초기화...")
        if not manager.init_metadata_tables():
            print("[ERROR] 메타데이터 테이블 초기화 실패")
            return False
        print("[OK] 메타데이터 테이블 준비 완료")
        
        # 2단계: 환경 관측 테이블 메타데이터 등록
        print("\n[2단계] 환경 관측(ENV_OBSERVATION) 메타데이터 등록...")
        if not manager.register_observation_metadata("env"):
            print("[ERROR] 환경 관측 메타데이터 등록 실패")
            return False
        print("[OK] 환경 관측 메타데이터 등록 완료")
        
        # 3단계: 생육 관측 테이블 메타데이터 등록
        print("\n[3단계] 생육 관측(GROWTH_OBSERVATION) 메타데이터 등록...")
        if not manager.register_observation_metadata("growth"):
            print("[ERROR] 생육 관측 메타데이터 등록 실패")
            return False
        print("[OK] 생육 관측 메타데이터 등록 완료")
        
        # 4단계: LLM/문서 처리 메타데이터 등록
        print("\n[4단계] LLM/문서 처리 메타데이터 등록...")
        if not manager.register_llm_metadata():
            print("[ERROR] LLM 메타데이터 등록 실패")
            return False
        print("[OK] LLM 메타데이터 등록 완료")
        
        # 5단계: 등록된 메타데이터 확인
        print("\n[5단계] 등록된 메타데이터 목록 확인...")
        metadatas = manager.list_all_metadata()
        print(f"[OK] 총 {len(metadatas)}개 메타데이터 조회:")
        
        for i, meta in enumerate(metadatas, 1):
            print(f"\n  [{i}] 테이블: {meta.table_name}")
            print(f"      소유자: {meta.owner}")
            print(f"      분류: {meta.classification.value}")
            print(f"      설명: {meta.description}")
            print(f"      컬럼 수: {len(meta.columns)}")
            print(f"      업데이트 주기: {meta.update_frequency}")
        
        print("\n" + "=" * 70)
        print("[완료] Phase 2 메타데이터 등록 성공!")
        print("=" * 70)
        
        # 통계 정보 출력
        print("\n[통계]")
        print(f"  - 총 메타데이터 등록: {len(metadatas)}개")
        print(f"  - 관측 테이블: 2개 (ENV_OBSERVATION, GROWTH_OBSERVATION)")
        print(f"  - LLM 관련 테이블: 3개 (document, embedding, llm_inference_log)")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] 테스트 실행 중 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_observation_metadata()
    sys.exit(0 if success else 1)
