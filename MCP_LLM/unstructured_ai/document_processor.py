#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unstructured AI - 문서 처리 모듈

PDF, 텍스트, 마크다운 등 비정형 문서를 청크로 변환하고,
메타데이터와 함께 데이터베이스에 저장합니다.
"""

import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import PyPDF2
import pymysql
from pydantic import BaseModel, Field


# ============================================================================
# Pydantic 모델
# ============================================================================

class DocumentChunk(BaseModel):
    """문서 청크 단위"""
    chunk_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_id: str
    chunk_no: int
    text: str
    token_count: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_dtm: datetime = Field(default_factory=datetime.now)


class Document(BaseModel):
    """문서 메타데이터"""
    doc_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_type: str  # manual, worklog, policy, spec, guidance
    title: str
    source_ref: str  # 원본 파일 경로 또는 URL
    chunks: List[DocumentChunk] = Field(default_factory=list)
    total_tokens: int = 0
    created_dtm: datetime = Field(default_factory=datetime.now)


# ============================================================================
# 문서 처리 클래스
# ============================================================================

class DocumentProcessor:
    """비정형 문서 처리 및 청킹 엔진"""
    
    def __init__(self, chunk_size: int = 512, chunk_overlap: int = 50):
        """
        초기화
        
        Args:
            chunk_size: 청크 토큰 수 (기본: 512)
            chunk_overlap: 청크 간 겹침 토큰 수 (기본: 50)
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.tokenizer = None
        
    def _estimate_tokens(self, text: str) -> int:
        """토큰 수 추정 (간단한 방식: 단어 수 / 0.75)"""
        words = text.split()
        return max(1, int(len(words) / 0.75))
    
    def _split_into_chunks(self, text: str, chunk_no_start: int = 0) -> List[Tuple[str, int]]:
        """
        텍스트를 청크로 분할
        
        Args:
            text: 입력 텍스트
            chunk_no_start: 청크 번호 시작값
            
        Returns:
            [(청크 텍스트, 청크 번호), ...] 리스트
        """
        sentences = text.split('.\n')  # 문장 단위로 분할
        if not sentences or sentences[-1]:
            sentences = text.split('. ')
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        chunk_no = chunk_no_start
        
        for sentence in sentences:
            if not sentence.strip():
                continue
            
            sentence_tokens = self._estimate_tokens(sentence)
            
            # 청크 크기 초과 시 새 청크 시작
            if current_tokens + sentence_tokens > self.chunk_size and current_chunk:
                chunk_text = '. '.join(current_chunk).strip()
                chunks.append((chunk_text, chunk_no))
                chunk_no += 1
                
                # 겹침 처리
                overlap_sentences = current_chunk[-max(1, len(current_chunk)//3):]
                current_chunk = overlap_sentences
                current_tokens = sum(self._estimate_tokens(s) for s in overlap_sentences)
            
            current_chunk.append(sentence.strip())
            current_tokens += sentence_tokens
        
        # 남은 텍스트 처리
        if current_chunk:
            chunk_text = '. '.join(current_chunk).strip()
            chunks.append((chunk_text, chunk_no))
        
        return chunks
    
    def process_text(self, text: str, doc_id: str, doc_type: str = "text") -> Document:
        """
        텍스트 문서 처리
        
        Args:
            text: 입력 텍스트
            doc_id: 문서 ID
            doc_type: 문서 타입
            
        Returns:
            Document 객체
        """
        doc = Document(
            doc_id=doc_id,
            doc_type=doc_type,
            title=f"TextDoc_{doc_id[:8]}",
            source_ref="memory://text"
        )
        
        chunks_data = self._split_into_chunks(text)
        total_tokens = 0
        
        for chunk_text, chunk_no in chunks_data:
            token_count = self._estimate_tokens(chunk_text)
            total_tokens += token_count
            
            chunk = DocumentChunk(
                doc_id=doc_id,
                chunk_no=chunk_no,
                text=chunk_text,
                token_count=token_count,
                metadata={
                    "source_type": "text",
                    "position": chunk_no,
                    "text_length": len(chunk_text)
                }
            )
            doc.chunks.append(chunk)
        
        doc.total_tokens = total_tokens
        return doc
    
    def process_pdf(self, pdf_path: str, doc_id: str = None, doc_type: str = "manual") -> Document:
        """
        PDF 파일 처리
        
        Args:
            pdf_path: PDF 파일 경로
            doc_id: 문서 ID (기본: 자동 생성)
            doc_type: 문서 타입
            
        Returns:
            Document 객체
        """
        if not doc_id:
            doc_id = str(uuid.uuid4())
        
        pdf_path = Path(pdf_path)
        
        try:
            # PDF 읽기
            text_content = ""
            with open(pdf_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                num_pages = len(pdf_reader.pages)
                
                for page_num, page in enumerate(pdf_reader.pages):
                    text = page.extract_text()
                    if text:
                        text_content += f"\n[Page {page_num+1}]\n{text}"
            
            # 문서 처리
            doc = Document(
                doc_id=doc_id,
                doc_type=doc_type,
                title=pdf_path.stem,
                source_ref=str(pdf_path)
            )
            
            chunks_data = self._split_into_chunks(text_content)
            total_tokens = 0
            
            for chunk_text, chunk_no in chunks_data:
                token_count = self._estimate_tokens(chunk_text)
                total_tokens += token_count
                
                chunk = DocumentChunk(
                    doc_id=doc_id,
                    chunk_no=chunk_no,
                    text=chunk_text,
                    token_count=token_count,
                    metadata={
                        "source_type": "pdf",
                        "source_file": pdf_path.name,
                        "text_length": len(chunk_text)
                    }
                )
                doc.chunks.append(chunk)
            
            doc.total_tokens = total_tokens
            return doc
            
        except Exception as e:
            print(f"[ERROR] PDF 처리 실패: {str(e)}")
            return None
    
    def process_markdown(self, md_text: str, doc_id: str = None, doc_type: str = "guidance") -> Document:
        """
        마크다운 문서 처리
        
        Args:
            md_text: 마크다운 텍스트
            doc_id: 문서 ID (기본: 자동 생성)
            doc_type: 문서 타입
            
        Returns:
            Document 객체
        """
        if not doc_id:
            doc_id = str(uuid.uuid4())
        
        # 마크다운 제목 추출
        lines = md_text.split('\n')
        title = "MarkdownDoc"
        for line in lines:
            if line.startswith('# '):
                title = line[2:].strip()
                break
        
        doc = Document(
            doc_id=doc_id,
            doc_type=doc_type,
            title=title,
            source_ref="memory://markdown"
        )
        
        chunks_data = self._split_into_chunks(md_text)
        total_tokens = 0
        
        for chunk_text, chunk_no in chunks_data:
            token_count = self._estimate_tokens(chunk_text)
            total_tokens += token_count
            
            chunk = DocumentChunk(
                doc_id=doc_id,
                chunk_no=chunk_no,
                text=chunk_text,
                token_count=token_count,
                metadata={
                    "source_type": "markdown",
                    "position": chunk_no
                }
            )
            doc.chunks.append(chunk)
        
        doc.total_tokens = total_tokens
        return doc


# ============================================================================
# 데이터베이스 저장 헬퍼
# ============================================================================

class DocumentStore:
    """문서 및 청크 저장소"""
    
    def __init__(self, host: str = "127.0.0.1", user: str = "root",
                 password: str = "dhwoan", database: str = "test"):
        """Initialize document store"""
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
    
    def save_document(self, doc: Document) -> bool:
        """
        문서 및 청크 저장
        
        Args:
            doc: Document 객체
            
        Returns:
            성공 여부
        """
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            # 문서 저장
            cur.execute("""
                INSERT INTO document (doc_id, doc_type, title, source_ref, created_dtm)
                VALUES (%s, %s, %s, %s, %s)
            """, [doc.doc_id, doc.doc_type, doc.title, doc.source_ref, doc.created_dtm])
            
            # 청크 저장
            for chunk in doc.chunks:
                cur.execute("""
                    INSERT INTO doc_chunk (chunk_id, doc_id, chunk_no, text_ref, token_count, created_dtm)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, [
                    chunk.chunk_id,
                    chunk.doc_id,
                    chunk.chunk_no,
                    f"s3://chunks/{chunk.chunk_id}.txt",  # 예시 저장 위치
                    chunk.token_count,
                    chunk.created_dtm
                ])
            
            conn.commit()
            print(f"[OK] 문서 저장 완료: {doc.title} ({len(doc.chunks)}개 청크)")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] 문서 저장 실패: {str(e)}")
            return False
        finally:
            conn.close()
    
    def get_document_chunks(self, doc_id: str) -> List[DocumentChunk]:
        """문서의 모든 청크 조회"""
        conn = self.connect()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT chunk_id, doc_id, chunk_no, text_ref, token_count, created_dtm
                FROM doc_chunk
                WHERE doc_id = %s
                ORDER BY chunk_no
            """, [doc_id])
            
            chunks = []
            for row in cur.fetchall():
                chunk = DocumentChunk(
                    chunk_id=row[0],
                    doc_id=row[1],
                    chunk_no=row[2],
                    text=row[3],  # 실제로는 text_ref에서 읽어야 함
                    token_count=row[4],
                    created_dtm=row[5]
                )
                chunks.append(chunk)
            
            return chunks
            
        except Exception as e:
            print(f"[ERROR] 청크 조회 실패: {str(e)}")
            return []
        finally:
            conn.close()
