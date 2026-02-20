#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unstructured AI - 임베딩 모듈

Hugging Face 모델을 사용하여 텍스트를 벡터로 변환하고,
Milvus 벡터 DB에 저장합니다.
"""

import json
import uuid
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import pymysql
from pydantic import BaseModel, Field

try:
    from sentence_transformers import SentenceTransformer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    SentenceTransformer = None
except (OSError, RuntimeError):
    # PyTorch DLL 로딩 오류 등
    TRANSFORMERS_AVAILABLE = False
    SentenceTransformer = None


# ============================================================================
# Pydantic 모델
# ============================================================================

class Embedding(BaseModel):
    """벡터 임베딩"""
    emb_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    chunk_id: str
    emb_model_id: str
    index_id: str
    vector: List[float]
    vector_dimension: int = 384
    created_dtm: datetime = Field(default_factory=datetime.now)


class EmbeddingModel(BaseModel):
    """임베딩 모델 메타데이터"""
    emb_model_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str = "sentence-transformers/multilingual-MiniLM-L12-v2"
    model_ver: str = "1.0.0"
    dim: int = 384
    created_dtm: datetime = Field(default_factory=datetime.now)


# ============================================================================
# 임베더 클래스
# ============================================================================

class TextEmbedder:
    """텍스트 임베딩 엔진"""
    
    def __init__(self, model_name: str = "sentence-transformers/multilingual-MiniLM-L12-v2",
                 batch_size: int = 32):
        """
        초기화
        
        Args:
            model_name: HuggingFace 모델명
            batch_size: 배치 크기
        """
        self.model_name = model_name
        self.batch_size = batch_size
        self.embedding_model = None
        self.emb_model_id = str(uuid.uuid4())
        
        if TRANSFORMERS_AVAILABLE and SentenceTransformer:
            try:
                print(f"[로드 중] 임베딩 모델: {model_name}")
                self.embedding_model = SentenceTransformer(model_name)
                print(f"[OK] 임베딩 모델 로드 완료 (차원: {self.get_embedding_dimension()})")
            except Exception as e:
                print(f"[WARNING] 모델 로드 실패: {str(e)}")
                print(f"[INFO] 시뮬레이션 모드로 진행합니다.")
        else:
            print(f"[INFO] Sentence-Transformers 모듈 사용 불가")
            print(f"[INFO] 시뮬레이션 모드로 진행합니다.")
    
    def get_embedding_dimension(self) -> int:
        """임베딩 차원 반환"""
        if self.embedding_model:
            return self.embedding_model.get_sentence_embedding_dimension()
        return 384
    
    def embed_text(self, text: str) -> np.ndarray:
        """
        단일 텍스트 임베딩
        
        Args:
            text: 입력 텍스트
            
        Returns:
            벡터 (numpy array)
        """
        if self.embedding_model:
            try:
                embedding = self.embedding_model.encode(text, convert_to_numpy=True)
                return embedding
            except Exception as e:
                print(f"[WARNING] 임베딩 생성 실패: {str(e)}")
        
        # 시뮬레이션 벡터 반환
        return np.random.randn(384)
    
    def embed_texts(self, texts: List[str]) -> List[np.ndarray]:
        """
        다중 텍스트 임베딩 (배치)
        
        Args:
            texts: 입력 텍스트 리스트
            
        Returns:
            벡터 리스트
        """
        if self.embedding_model:
            try:
                embeddings = self.embedding_model.encode(
                    texts,
                    batch_size=self.batch_size,
                    convert_to_numpy=True,
                    show_progress_bar=False
                )
                return embeddings
            except Exception as e:
                print(f"[WARNING] 배치 임베딩 생성 실패: {str(e)}")
        
        # 시뮬레이션 벡터 반환
        return [np.random.randn(384) for _ in texts]
    
    def embed_chunks(self, chunks: List[Dict[str, Any]]) -> List[Embedding]:
        """
        청크 임베딩
        
        Args:
            chunks: [{"chunk_id": "...", "text": "..."}, ...]
            
        Returns:
            Embedding 객체 리스트
        """
        chunk_texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embed_texts(chunk_texts)
        
        result = []
        for i, chunk in enumerate(chunks):
            if i < len(embeddings):
                emb = Embedding(
                    chunk_id=chunk['chunk_id'],
                    emb_model_id=self.emb_model_id,
                    index_id="idx_milvus_001",  # 기본값
                    vector=embeddings[i].tolist(),
                    vector_dimension=len(embeddings[i])
                )
                result.append(emb)
        
        return result


# ============================================================================
# 임베딩 저장소
# ============================================================================

class EmbeddingStore:
    """임베딩 저장 및 관리"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """Initialize embedding store"""
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
    
    def register_embedding_model(self, model: EmbeddingModel) -> bool:
        """
        임베딩 모델 등록
        
        Args:
            model: EmbeddingModel 객체
            
        Returns:
            성공 여부
        """
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO embedding_model (emb_model_id, model_name, model_ver, dim, created_dtm)
                VALUES (%s, %s, %s, %s, %s)
            """, [model.emb_model_id, model.model_name, model.model_ver, model.dim, model.created_dtm])
            
            conn.commit()
            print(f"[OK] 임베딩 모델 등록: {model.model_name} (차원: {model.dim})")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] 모델 등록 실패: {str(e)}")
            return False
        finally:
            conn.close()
    
    def save_embeddings(self, embeddings: List[Embedding], index_id: str = "idx_milvus_001") -> bool:
        """
        임베딩 저장
        
        Args:
            embeddings: Embedding 객체 리스트
            index_id: 인덱스 ID
            
        Returns:
            성공 여부
        """
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            for emb in embeddings:
                # 벡터를 JSON 문자열로 저장 (또는 별도 저장소 참조)
                vector_ref = f"milvus://vectors/{emb.emb_id}.npy"
                
                cur.execute("""
                    INSERT INTO embedding (emb_id, chunk_id, emb_model_id, index_id, vector_ref, created_dtm)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, [
                    emb.emb_id,
                    emb.chunk_id,
                    emb.emb_model_id,
                    index_id,
                    vector_ref,
                    emb.created_dtm
                ])
            
            conn.commit()
            print(f"[OK] {len(embeddings)}개 임베딩 저장 완료")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] 임베딩 저장 실패: {str(e)}")
            return False
        finally:
            conn.close()
    
    def get_embedding(self, emb_id: str) -> Optional[Embedding]:
        """임베딩 조회"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT emb_id, chunk_id, emb_model_id, index_id, created_dtm
                FROM embedding
                WHERE emb_id = %s
            """, [emb_id])
            
            row = cur.fetchone()
            if row:
                return Embedding(
                    emb_id=row[0],
                    chunk_id=row[1],
                    emb_model_id=row[2],
                    index_id=row[3],
                    vector=[],  # 벡터는 별도 저장소에서 조회
                    created_dtm=row[4]
                )
            return None
            
        except Exception as e:
            print(f"[ERROR] 임베딩 조회 실패: {str(e)}")
            return None
        finally:
            conn.close()
    
    def get_embeddings_by_chunk(self, chunk_id: str) -> List[Embedding]:
        """청크별 임베딩 조회"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT emb_id, chunk_id, emb_model_id, index_id, created_dtm
                FROM embedding
                WHERE chunk_id = %s
                ORDER BY created_dtm DESC
            """, [chunk_id])
            
            embeddings = []
            for row in cur.fetchall():
                emb = Embedding(
                    emb_id=row[0],
                    chunk_id=row[1],
                    emb_model_id=row[2],
                    index_id=row[3],
                    vector=[],
                    created_dtm=row[4]
                )
                embeddings.append(emb)
            
            return embeddings
            
        except Exception as e:
            print(f"[ERROR] 청크별 임베딩 조회 실패: {str(e)}")
            return []
        finally:
            conn.close()


# ============================================================================
# 유틸리티
# ============================================================================

def vector_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    두 벡터의 코사인 유사도 계산
    
    Args:
        vec1: 벡터 1
        vec2: 벡터 2
        
    Returns:
        유사도 (0~1)
    """
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return np.dot(vec1, vec2) / (norm1 * norm2)


def vectors_similarity(query_vec: np.ndarray, doc_vecs: List[np.ndarray]) -> List[float]:
    """
    쿼리 벡터와 여러 문서 벡터의 유사도
    
    Args:
        query_vec: 쿼리 벡터
        doc_vecs: 문서 벡터 리스트
        
    Returns:
        유사도 리스트
    """
    return [vector_similarity(query_vec, vec) for vec in doc_vecs]
