"""
MCP Context 시스템 - 데이터 허브 거버넌스 플랫폼

표준·메타데이터·품질·거버넌스가 내재화된 데이터 허브
LLM/MCP를 위한 자동화·지능화 계층
"""

__version__ = "1.0.0"
__author__ = "Data Governance Team"

from hub_governance.metadata_manager import (
    MetadataManager,
    TableMetadata,
    ColumnMetadata,
    DataQualityScore,
    DataClassification
)

from hub_governance.quality_engine import (
    QualityEngine,
    QualityRule,
    QualityCheckResult,
    RuleSeverity
)

from hub_governance.lineage_tracker import (
    LineageTracker,
    LineageDAG,
    LineageNode,
    LineageEdge,
    TransformationType
)

from hub_governance.context_builder import (
    MCPContextBuilder,
    ContextPackage
)

__all__ = [
    # Metadata
    "MetadataManager",
    "TableMetadata",
    "ColumnMetadata",
    "DataQualityScore",
    "DataClassification",
    
    # Quality
    "QualityEngine",
    "QualityRule",
    "QualityCheckResult",
    "RuleSeverity",
    
    # Lineage
    "LineageTracker",
    "LineageDAG",
    "LineageNode",
    "LineageEdge",
    "TransformationType",
    
    # Context
    "MCPContextBuilder",
    "ContextPackage"
]
