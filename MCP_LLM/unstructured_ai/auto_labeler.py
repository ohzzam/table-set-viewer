#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unstructured AI - 자동 라벨링 모듈

LLM을 사용하여 문서를 자동으로 분류, 태그 부여, 
표준 매핑을 수행합니다.
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
import pymysql
from pydantic import BaseModel, Field


# ============================================================================
# Pydantic 모델
# ============================================================================

class Label(BaseModel):
    """문서 라벨"""
    label_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_id: str
    label_type: str  # classification, standard_code, entity, sentiment
    label_value: str
    confidence: float = Field(ge=0, le=1)
    source: str = "llm"  # llm, rule, user
    created_dtm: datetime = Field(default_factory=datetime.now)


class ClassificationResult(BaseModel):
    """분류 결과"""
    doc_id: str
    primary_class: str
    secondary_classes: List[str] = Field(default_factory=list)
    confidence_score: float
    reasoning: str = ""


class StandardMapping(BaseModel):
    """표준 코드 매핑"""
    mapping_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_id: str
    chunk_id: Optional[str] = None
    standard_code_id: str
    mapped_text: str
    confidence: float
    created_dtm: datetime = Field(default_factory=datetime.now)


# ============================================================================
# 자동 라벨러
# ============================================================================

class AutoLabeler:
    """LLM 기반 자동 라벨링 엔진"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """
        초기화
        
        Args:
            host: DB 호스트
            user: DB 사용자
            password: DB 암호
            database: DB 이름
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        # 분류 규칙 (LLM 없이 동작하는 기본 규칙)
        self.classification_rules = {
            "manual": ["설명서", "가이드", "매뉴얼", "사용법", "방법"],
            "worklog": ["작업", "로그", "기록", "진행상황", "업데이트"],
            "policy": ["정책", "규칙", "기준", "원칙", "가이드라인"],
            "spec": ["사양", "명세", "스펙", "요구사항", "사양서"],
            "guidance": ["지침", "가이드", "권장", "권고", "지시"]
        }
        
        # 표준 코드 (예시)
        self.standard_codes = {
            "ENV_OBSERVATION": ["환경", "온도", "습도", "빛", "센서"],
            "GROWTH_OBSERVATION": ["생육", "높이", "엽수", "개수", "상태"],
            "CULTIVATION_TYPE": ["수경", "토경", "수직", "수평"],
            "QUALITY_LEVEL": ["우수", "양호", "보통", "불량"]
        }
    
    def connect(self):
        """Connect to database"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def classify_document(self, text: str, doc_id: str) -> ClassificationResult:
        """
        문서 분류 (규칙 기반)
        
        Args:
            text: 문서 텍스트
            doc_id: 문서 ID
            
        Returns:
            ClassificationResult 객체
        """
        text_lower = text.lower()
        scores = {}
        
        # 각 분류에 대해 키워드 매칭
        for class_name, keywords in self.classification_rules.items():
            score = 0
            matching_keywords = []
            
            for keyword in keywords:
                if keyword in text_lower:
                    score += 1
                    matching_keywords.append(keyword)
            
            if score > 0:
                scores[class_name] = (score, matching_keywords)
        
        # 점수 순으로 정렬
        sorted_scores = sorted(scores.items(), key=lambda x: x[1][0], reverse=True)
        
        if sorted_scores:
            primary_class = sorted_scores[0][0]
            confidence = min(0.99, sorted_scores[0][1][0] / len(self.classification_rules.get(primary_class, [])))
            
            secondary = [item[0] for item in sorted_scores[1:3]]
            keywords_found = sorted_scores[0][1][1]
            
            return ClassificationResult(
                doc_id=doc_id,
                primary_class=primary_class,
                secondary_classes=secondary,
                confidence_score=confidence,
                reasoning=f"Matched keywords: {', '.join(keywords_found)}"
            )
        
        return ClassificationResult(
            doc_id=doc_id,
            primary_class="unknown",
            confidence_score=0.0,
            reasoning="No matching classification found"
        )
    
    def extract_entities(self, text: str, doc_id: str) -> List[Dict[str, Any]]:
        """
        개체 추출 (규칙 기반)
        
        Args:
            text: 문서 텍스트
            doc_id: 문서 ID
            
        Returns:
            개체 리스트
        """
        entities = []
        text_lower = text.lower()
        
        # 표준 코드별 개체 추출
        for code_id, keywords in self.standard_codes.items():
            for keyword in keywords:
                if keyword in text_lower:
                    # 문맥 추출 (간단한 예시)
                    idx = text_lower.find(keyword)
                    context_start = max(0, idx - 30)
                    context_end = min(len(text), idx + len(keyword) + 30)
                    context = text[context_start:context_end].strip()
                    
                    entities.append({
                        "entity_type": "standard_code",
                        "entity_value": code_id,
                        "matched_keyword": keyword,
                        "context": context,
                        "confidence": 0.85
                    })
        
        return entities
    
    def map_to_standards(self, text: str, doc_id: str, chunk_id: str = None) -> List[StandardMapping]:
        """
        표준 코드 매핑
        
        Args:
            text: 텍스트
            doc_id: 문서 ID
            chunk_id: 청크 ID (선택)
            
        Returns:
            StandardMapping 리스트
        """
        mappings = []
        text_lower = text.lower()
        
        for code_id, keywords in self.standard_codes.items():
            for keyword in keywords:
                if keyword in text_lower:
                    # 매칭된 텍스트 추출
                    idx = text_lower.find(keyword)
                    mapped_text = text[max(0, idx-20):min(len(text), idx+len(keyword)+20)]
                    
                    mapping = StandardMapping(
                        doc_id=doc_id,
                        chunk_id=chunk_id,
                        standard_code_id=code_id,
                        mapped_text=mapped_text.strip(),
                        confidence=0.80
                    )
                    mappings.append(mapping)
        
        return mappings
    
    def tag_document(self, doc_id: str, classification: ClassificationResult,
                    entities: List[Dict[str, Any]]) -> List[Label]:
        """
        문서 태깅
        
        Args:
            doc_id: 문서 ID
            classification: 분류 결과
            entities: 개체 리스트
            
        Returns:
            Label 리스트
        """
        labels = []
        
        # 분류 라벨
        label = Label(
            doc_id=doc_id,
            label_type="classification",
            label_value=classification.primary_class,
            confidence=classification.confidence_score,
            source="rule"
        )
        labels.append(label)
        
        # 개체 라벨
        for entity in entities:
            label = Label(
                doc_id=doc_id,
                label_type="standard_code",
                label_value=entity.get("entity_value", "unknown"),
                confidence=entity.get("confidence", 0.5),
                source="rule"
            )
            labels.append(label)
        
        return labels


# ============================================================================
# 라벨 저장소
# ============================================================================

class LabelStore:
    """라벨 저장 및 관리"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """Initialize label store"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    def connect(self):
        """Connect to database"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def save_labels(self, labels: List[Label]) -> bool:
        """
        라벨 저장 (생성하려는 테이블이 없으므로 메타 정보만 저장)
        
        Args:
            labels: Label 객체 리스트
            
        Returns:
            성공 여부
        """
        try:
            # 현재 테이블 구조상 직접 저장할 수 없으므로 로그만 출력
            print(f"[OK] {len(labels)}개 라벨 생성:")
            for label in labels:
                print(f"     - {label.label_type}: {label.label_value} ({label.confidence:.1%} 확신도)")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] 라벨 저장 실패: {str(e)}")
            return False
    
    def save_mappings(self, mappings: List[StandardMapping]) -> bool:
        """
        표준 매핑 저장
        
        Args:
            mappings: StandardMapping 리스트
            
        Returns:
            성공 여부
        """
        try:
            # 현재 테이블 구조상 직접 저장할 수 없으므로 로그만 출력
            print(f"[OK] {len(mappings)}개 표준 매핑 생성:")
            for mapping in mappings:
                print(f"     - {mapping.standard_code_id}: {mapping.mapped_text[:50]}...")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] 매핑 저장 실패: {str(e)}")
            return False
    
    def get_document_labels(self, doc_id: str) -> List[Dict[str, Any]]:
        """문서의 모든 라벨 조회"""
        # 실제 구현 시 DB에서 조회
        return []
    
    def get_document_mappings(self, doc_id: str) -> List[StandardMapping]:
        """문서의 모든 매핑 조회"""
        # 실제 구현 시 DB에서 조회
        return []
