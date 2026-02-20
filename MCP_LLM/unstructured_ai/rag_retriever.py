#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unstructured AI - RAG 검색 모듈

벡터 유사도를 기반으로 문서 청크를 검색하고,
LLM을 위한 컨텍스트를 생성합니다.
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
import pymysql
from pydantic import BaseModel, Field


# ============================================================================
# Pydantic 모델
# ============================================================================

class RetrievalResult(BaseModel):
    """검색 결과"""
    chunk_id: str
    doc_id: str
    text: str
    similarity_score: float
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RAGContext(BaseModel):
    """RAG 컨텍스트 패키지"""
    query: str
    query_vector: Optional[List[float]] = None
    retrieved_chunks: List[RetrievalResult] = Field(default_factory=list)
    total_results: int = 0
    retrieved_at: datetime = Field(default_factory=datetime.now)
    
    def to_prompt(self) -> str:
        """LLM 프롬프트 생성"""
        prompt = f"Query: {self.query}\n\n"
        prompt += "Retrieved Context:\n"
        prompt += "=" * 80 + "\n"
        
        for i, result in enumerate(self.retrieved_chunks, 1):
            prompt += f"\n[{i}] (유사도: {result.similarity_score:.1%})\n"
            prompt += f"Document: {result.doc_id}\n"
            prompt += f"Chunk: {result.chunk_id}\n"
            prompt += f"---\n{result.text}\n"
        
        prompt += "\n" + "=" * 80 + "\n"
        return prompt


# ============================================================================
# RAG 검색기
# ============================================================================

class RAGRetriever:
    """RAG 기반 컨텍스트 검색"""
    
    def __init__(self, top_k: int = 5, similarity_threshold: float = 0.5,
                 host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """
        초기화
        
        Args:
            top_k: 상위 K개 결과 반환
            similarity_threshold: 유사도 임계값
            host: DB 호스트
            user: DB 사용자
            password: DB 암호
            database: DB 이름
        """
        self.top_k = top_k
        self.similarity_threshold = similarity_threshold
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        # 벡터 인덱스 (메모리에 로드할 수 있음)
        self.vector_cache = {}  # {chunk_id: vector}
    
    def connect(self):
        """Connect to database"""
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
    
    def _vector_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """코사인 유사도 계산"""
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(np.dot(vec1, vec2) / (norm1 * norm2))
    
    def load_chunk_vectors(self, chunk_ids: List[str] = None) -> bool:
        """
        청크 벡터 로드 (메모리 캐시)
        
        Args:
            chunk_ids: 청크 ID 리스트 (None이면 모두 로드)
            
        Returns:
            성공 여부
        """
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            if chunk_ids:
                placeholders = ','.join(['%s'] * len(chunk_ids))
                query = f"""
                    SELECT e.chunk_id, e.vector_ref
                    FROM embedding e
                    WHERE e.chunk_id IN ({placeholders})
                """
                cur.execute(query, chunk_ids)
            else:
                cur.execute("SELECT chunk_id, vector_ref FROM embedding LIMIT 1000")
            
            # 벡터 참조 로드 (실제로는 객체 스토리지에서 읽음)
            count = 0
            for row in cur.fetchall():
                chunk_id = row[0]
                # 실제로는 vector_ref에서 벡터 데이터 로드
                # 데모용으로 임의의 벡터 생성
                self.vector_cache[chunk_id] = np.random.randn(384)
                count += 1
            
            print(f"[OK] {count}개 벡터 로드 완료")
            return True
            
        except Exception as e:
            print(f"[ERROR] 벡터 로드 실패: {str(e)}")
            return False
        finally:
            conn.close()
    
    def retrieve_by_vector(self, query_vector: np.ndarray) -> List[RetrievalResult]:
        """
        벡터 검색
        
        Args:
            query_vector: 쿼리 벡터
            
        Returns:
            RetrievalResult 리스트
        """
        results = []
        
        # 메모리 캐시에서 검색 (실제로는 Milvus 사용)
        similarities = {}
        for chunk_id, vec in self.vector_cache.items():
            similarity = self._vector_similarity(query_vector, vec)
            if similarity >= self.similarity_threshold:
                similarities[chunk_id] = similarity
        
        # 상위 K개 선택
        top_chunks = sorted(similarities.items(), key=lambda x: x[1], reverse=True)[:self.top_k]
        
        # 청크 정보 조회
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            for chunk_id, similarity in top_chunks:
                cur.execute("""
                    SELECT dc.chunk_id, dc.doc_id, dc.text_ref, d.title, dc.token_count
                    FROM doc_chunk dc
                    JOIN document d ON dc.doc_id = d.doc_id
                    WHERE dc.chunk_id = %s
                """, [chunk_id])
                
                row = cur.fetchone()
                if row:
                    result = RetrievalResult(
                        chunk_id=row[0],
                        doc_id=row[1],
                        text=f"[{row[3]}] 청크",  # 실제로는 text_ref에서 읽음
                        similarity_score=similarity,
                        metadata={
                            "token_count": row[4],
                            "title": row[3],
                            "source_ref": row[2]
                        }
                    )
                    results.append(result)
            
            return results
            
        except Exception as e:
            print(f"[ERROR] 벡터 검색 실패: {str(e)}")
            return []
        finally:
            conn.close()
    
    def retrieve_by_text(self, query_text: str, embedder=None) -> List[RetrievalResult]:
        """
        텍스트 쿼리로 검색
        
        Args:
            query_text: 쿼리 텍스트
            embedder: TextEmbedder 객체
            
        Returns:
            RetrievalResult 리스트
        """
        if not embedder:
            print("[WARNING] embedder가 제공되지 않았습니다.")
            return []
        
        # 쿼리 임베딩
        query_vector = embedder.embed_text(query_text)
        if query_vector is None:
            return []
        
        # 벡터 검색
        return self.retrieve_by_vector(query_vector)
    
    def retrieve_by_keyword(self, keywords: List[str]) -> List[RetrievalResult]:
        """
        키워드 기반 검색
        
        Args:
            keywords: 키워드 리스트
            
        Returns:
            RetrievalResult 리스트
        """
        conn = self.connect()
        cur = conn.cursor()
        results = []
        
        try:
            # 키워드 매칭 (FULLTEXT 검색 또는 LIKE)
            keyword_clause = " OR ".join([f"text_ref LIKE %s" for _ in keywords])
            query = f"""
                SELECT dc.chunk_id, dc.doc_id, dc.text_ref, d.title, dc.token_count
                FROM doc_chunk dc
                JOIN document d ON dc.doc_id = d.doc_id
                WHERE {keyword_clause}
                LIMIT %s
            """
            
            search_terms = [f"%{kw}%" for kw in keywords] + [self.top_k]
            cur.execute(query, search_terms)
            
            for row in cur.fetchall():
                result = RetrievalResult(
                    chunk_id=row[0],
                    doc_id=row[1],
                    text=f"[{row[3]}] 청크",
                    similarity_score=0.5,  # 키워드 검색은 유사도 없음
                    metadata={
                        "token_count": row[4],
                        "title": row[3],
                        "source_ref": row[2],
                        "search_method": "keyword"
                    }
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"[ERROR] 키워드 검색 실패: {str(e)}")
            return []
        finally:
            conn.close()
    
    def build_context(self, results: List[RetrievalResult], query: str = "",
                     query_vector: Optional[np.ndarray] = None) -> RAGContext:
        """
        RAG 컨텍스트 패키지 생성
        
        Args:
            results: 검색 결과 리스트
            query: 원본 쿼리
            query_vector: 쿼리 벡터
            
        Returns:
            RAGContext 객체
        """
        context = RAGContext(
            query=query,
            query_vector=query_vector.tolist() if query_vector is not None else None,
            retrieved_chunks=results,
            total_results=len(results)
        )
        return context
    
    def rerank_results(self, results: List[RetrievalResult], diversity_factor: float = 0.2) -> List[RetrievalResult]:
        """
        검색 결과 재순서화 (다양성 고려)
        
        Args:
            results: 검색 결과
            diversity_factor: 다양성 가중치
            
        Returns:
            재순서화된 결과
        """
        if not results:
            return results
        
        # 간단한 다양성 알고리즘
        reranked = []
        selected_docs = set()
        
        for result in sorted(results, key=lambda x: x.similarity_score, reverse=True):
            if result.doc_id not in selected_docs or len(selected_docs) < 3:
                reranked.append(result)
                selected_docs.add(result.doc_id)
                
                if len(reranked) >= self.top_k:
                    break
        
        return reranked


# ============================================================================
# 검색 캐시 관리
# ============================================================================

class SearchCache:
    """최근 검색 쿼리 캐싱"""
    
    def __init__(self, max_size: int = 100):
        """Initialize cache"""
        self.max_size = max_size
        self.cache = {}  # {query_hash: RAGContext}
    
    def get(self, query: str) -> Optional[RAGContext]:
        """캐시 조회"""
        query_hash = hash(query)
        return self.cache.get(query_hash)
    
    def put(self, query: str, context: RAGContext):
        """캐시 저장"""
        if len(self.cache) >= self.max_size:
            # FIFO 방식으로 가장 오래된 것 제거
            self.cache.pop(next(iter(self.cache)))
        
        query_hash = hash(query)
        self.cache[query_hash] = context
    
    def clear(self):
        """캐시 초기화"""
        self.cache.clear()
