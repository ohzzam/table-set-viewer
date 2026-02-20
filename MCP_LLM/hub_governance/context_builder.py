"""
MCP Context 생성기

LLM이 사용할 수 있는 구조화된 Context 패키지 생성
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json
from hub_governance.metadata_manager import MetadataManager, TableMetadata
from hub_governance.quality_engine import QualityEngine
from hub_governance.lineage_tracker import LineageTracker
from pydantic import BaseModel, Field


class ContextPackage(BaseModel):
    """LLM을 위한 Context 패키지"""
    package_id: str
    package_name: str
    generated_at: datetime = Field(default_factory=datetime.now)
    
    # 메타데이터 컨텍스트
    metadata_context: Dict[str, Any] = Field(default_factory=dict)
    
    # 거버넌스 컨텍스트
    governance_context: Dict[str, Any] = Field(default_factory=dict)
    
    # 품질 컨텍스트
    quality_context: Dict[str, Any] = Field(default_factory=dict)
    
    # 라인리지 컨텍스트
    lineage_context: Dict[str, Any] = Field(default_factory=dict)
    
    # 컨텍스트 메타정보
    context_metadata: Dict[str, Any] = Field(default_factory=dict)


class MCPContextBuilder:
    """MCP Context 빌더"""
    
    def __init__(self):
        """컨텍스트 빌더 초기화"""
        self.metadata_mgr = MetadataManager()
        self.quality_engine = QualityEngine()
        self.lineage_tracker = LineageTracker()
    
    def build_metadata_context(self, table_ids: List[str] = None) -> Dict[str, Any]:
        """메타데이터 컨텍스트 생성"""
        
        # 모든 메타데이터 조회
        if table_ids:
            metadatas = []
            for table_id in table_ids:
                metadata = self.metadata_mgr.get_table_metadata(table_id)
                if metadata:
                    metadatas.append(metadata)
        else:
            metadatas = self.metadata_mgr.list_all_metadata()
        
        context = {
            "total_assets": len(metadatas),
            "assets": []
        }
        
        for metadata in metadatas:
            asset_context = {
                "table_id": metadata.table_id,
                "table_name": metadata.table_name,
                "database_name": metadata.database_name,
                "description": metadata.description,
                "owner": metadata.owner,
                "owner_email": metadata.owner_email,
                "classification": metadata.classification.value,
                "version": metadata.version,
                "update_frequency": metadata.update_frequency,
                "row_count": metadata.row_count,
                "size_mb": metadata.size_mb,
                "data_dictionary": []
            }
            
            # 컬럼 정보
            for column in metadata.columns:
                asset_context["data_dictionary"].append({
                    "column_name": column.column_name,
                    "data_type": column.data_type,
                    "nullable": column.nullable,
                    "description": column.description,
                    "classification": column.classification.value,
                    "example_values": column.example_values[:3]  # 처음 3개만
                })
            
            context["assets"].append(asset_context)
        
        return context
    
    def build_quality_context(self, table_ids: List[str] = None) -> Dict[str, Any]:
        """품질 컨텍스트 생성"""
        
        if not table_ids:
            table_ids = [m.table_id for m in self.metadata_mgr.list_all_metadata()]
        
        context = {
            "quality_status": "UNKNOWN",
            "critical_issues": [],
            "tables": []
        }
        
        total_score = 0
        critical_count = 0
        
        for table_id in table_ids:
            # 각 테이블의 품질 규칙 실행
            results = self.quality_engine.execute_all_rules(table_id)
            
            table_quality = {
                "table_id": table_id,
                "rules_executed": len(results),
                "passed_rules": sum(1 for r in results if r.passed),
                "failed_rules": sum(1 for r in results if not r.passed),
                "checks": []
            }
            
            for result in results:
                check = {
                    "rule_id": result.rule_id,
                    "rule_name": result.rule_name,
                    "passed": result.passed,
                    "score": result.score,
                    "threshold": result.threshold,
                    "message": result.message
                }
                table_quality["checks"].append(check)
                
                if not result.passed and result.details.get("severity") == "critical":
                    context["critical_issues"].append({
                        "table_id": table_id,
                        "rule_id": result.rule_id,
                        "issue": result.message
                    })
                    critical_count += 1
                
                total_score += result.score
            
            context["tables"].append(table_quality)
        
        # 전체 품질 상태 판단
        avg_score = total_score / max(sum(len(t.get("checks", [])) for t in context["tables"]), 1)
        
        if critical_count > 0:
            context["quality_status"] = "CRITICAL"
        elif avg_score >= 90:
            context["quality_status"] = "EXCELLENT"
        elif avg_score >= 80:
            context["quality_status"] = "GOOD"
        elif avg_score >= 70:
            context["quality_status"] = "FAIR"
        else:
            context["quality_status"] = "POOR"
        
        context["average_quality_score"] = round(avg_score, 2)
        context["critical_issue_count"] = critical_count
        
        return context
    
    def build_lineage_context(self, table_ids: List[str] = None) -> Dict[str, Any]:
        """라인리지 컨텍스트 생성"""
        
        context = {
            "nodes_total": 0,
            "edges_total": 0,
            "root_sources": [],
            "leaf_targets": [],
            "table_lineage": {}
        }
        
        if not table_ids:
            table_ids = [m.table_id for m in self.metadata_mgr.list_all_metadata()]
        
        for table_id in table_ids:
            upstream = self.lineage_tracker.get_upstream_nodes(table_id)
            downstream = self.lineage_tracker.get_downstream_nodes(table_id)
            
            table_lineage = {
                "table_id": table_id,
                "data_sources": [],
                "data_consumers": []
            }
            
            for node in upstream:
                table_lineage["data_sources"].append({
                    "node_id": node.node_id,
                    "node_name": node.node_name,
                    "node_type": node.node_type,
                    "table_name": node.table_name
                })
            
            for node in downstream:
                table_lineage["data_consumers"].append({
                    "node_id": node.node_id,
                    "node_name": node.node_name,
                    "node_type": node.node_type,
                    "table_name": node.table_name
                })
            
            context["table_lineage"][table_id] = table_lineage
            
            context["nodes_total"] += 1
            context["root_sources"].extend([n["node_id"] for n in table_lineage["data_sources"]])
            context["leaf_targets"].extend([n["node_id"] for n in table_lineage["data_consumers"]])
        
        context["root_sources"] = list(set(context["root_sources"]))
        context["leaf_targets"] = list(set(context["leaf_targets"]))
        
        return context
    
    def build_governance_context(self) -> Dict[str, Any]:
        """거버넌스 컨텍스트 생성"""
        
        metadatas = self.metadata_mgr.list_all_metadata()
        
        # 소유자별 집계
        ownership_map = {}
        for metadata in metadatas:
            if metadata.owner not in ownership_map:
                ownership_map[metadata.owner] = []
            ownership_map[metadata.owner].append({
                "table_id": metadata.table_id,
                "table_name": metadata.table_name
            })
        
        # 분류별 집계
        classification_map = {}
        for metadata in metadatas:
            class_value = metadata.classification.value
            if class_value not in classification_map:
                classification_map[class_value] = 0
            classification_map[class_value] += 1
        
        context = {
            "total_assets": len(metadatas),
            "ownership": ownership_map,
            "classification_distribution": classification_map,
            "data_governance_policies": {
                "classification_levels": ["public", "internal", "confidential", "restricted"],
                "retention_policy": "Based on classification level",
                "access_control": "Role-based access control (RBAC)"
            }
        }
        
        return context
    
    def build_complete_context(self, package_id: str, package_name: str, 
                              table_ids: List[str] = None) -> ContextPackage:
        """완전한 Context 패키지 생성"""
        
        print(f"Building context package: {package_name}...")
        
        context_pkg = ContextPackage(
            package_id=package_id,
            package_name=package_name,
            metadata_context=self.build_metadata_context(table_ids),
            quality_context=self.build_quality_context(table_ids),
            lineage_context=self.build_lineage_context(table_ids),
            governance_context=self.build_governance_context(),
            context_metadata={
                "builder_version": "1.0.0",
                "tables_included": len(table_ids) if table_ids else "all",
                "context_layers": [
                    "metadata_context",
                    "quality_context",
                    "lineage_context",
                    "governance_context"
                ]
            }
        )
        
        print(f"Context package built successfully!")
        return context_pkg
    
    def export_context_to_json(self, context_pkg: ContextPackage, 
                              filepath: str) -> bool:
        """Context 패키지를 JSON으로 내보내기"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(context_pkg.model_dump(default=str), f, 
                         ensure_ascii=False, indent=2)
            print(f"Context exported to: {filepath}")
            return True
        except Exception as e:
            print(f"Export error: {e}")
            return False
    
    def export_context_for_llm_prompt(self, context_pkg: ContextPackage) -> str:
        """LLM 프롬프트용 Context 텍스트 생성"""
        
        prompt = f"""
## 데이터 허브 정보

### 메타데이터
- 총 데이터 자산: {context_pkg.metadata_context.get('total_assets', 0)}개
- 보유 데이터베이스:
"""
        
        for asset in context_pkg.metadata_context.get('assets', []):
            prompt += f"\n  - **{asset['table_name']}** ({asset['database_name']})\n"
            prompt += f"    - 설명: {asset.get('description', 'N/A')}\n"
            prompt += f"    - 소유자: {asset['owner']}\n"
            prompt += f"    - 레코드 수: {asset['row_count']:,}\n"
            prompt += f"    - 크기: {asset['size_mb']} MB\n"
            prompt += f"    - 컬럼:\n"
            for col in asset['data_dictionary'][:5]:  # 처음 5개 컬럼만
                prompt += f"      - {col['column_name']} ({col['data_type']})\n"

        prompt += f"\n### 데이터 품질\n"
        prompt += f"- 전체 품질 상태: {context_pkg.quality_context.get('quality_status', 'UNKNOWN')}\n"
        prompt += f"- 평균 품질 점수: {context_pkg.quality_context.get('average_quality_score', 0)}%\n"
        if context_pkg.quality_context.get('critical_issues'):
            prompt += f"- 중요 이슈:\n"
            for issue in context_pkg.quality_context['critical_issues'][:5]:
                prompt += f"  - {issue['issue']}\n"
        
        prompt += f"\n### 거버넌스\n"
        prompt += f"- 데이터 소유 부서:\n"
        for owner, tables in context_pkg.governance_context.get('ownership', {}).items():
            prompt += f"  - {owner}: {len(tables)}개 테이블\n"
        
        return prompt
