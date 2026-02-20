#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unstructured AI 패키지 - 비정형 데이터 처리

모듈:
- document_processor: 문서 청킹
- embedder: 벡터 임베딩
- auto_labeler: 자동 라벨링
- rag_retriever: RAG 검색
"""

from .document_processor import (
    DocumentProcessor,
    DocumentStore,
    Document,
    DocumentChunk
)

from .embedder import (
    TextEmbedder,
    EmbeddingStore,
    Embedding,
    EmbeddingModel,
    vector_similarity,
    vectors_similarity
)

from .auto_labeler import (
    AutoLabeler,
    LabelStore,
    Label,
    ClassificationResult,
    StandardMapping
)

from .rag_retriever import (
    RAGRetriever,
    RAGContext,
    RetrievalResult,
    SearchCache
)

__all__ = [
    # document_processor
    "DocumentProcessor",
    "DocumentStore",
    "Document",
    "DocumentChunk",
    
    # embedder
    "TextEmbedder",
    "EmbeddingStore",
    "Embedding",
    "EmbeddingModel",
    "vector_similarity",
    "vectors_similarity",
    
    # auto_labeler
    "AutoLabeler",
    "LabelStore",
    "Label",
    "ClassificationResult",
    "StandardMapping",
    
    # rag_retriever
    "RAGRetriever",
    "RAGContext",
    "RetrievalResult",
    "SearchCache",
]

__version__ = "1.0.0"
__author__ = "Data Hub Team"
