-- =============================================================================
-- Phase 2: 관측 데이터 + LLM/MCP 물리 테이블 DDL
-- 데이터 허브: 표준/메타데이터/품질/거버넌스 플랫폼 (RFP 준수)
-- =============================================================================

-- ============ 3.2 관측 테이블(월 파티셔닝) ============

-- 3.2.1 환경 관측(ENV_OBSERVATION)
CREATE TABLE IF NOT EXISTS env_observation (
  env_obs_uuid   CHAR(36) NOT NULL,
  stream_id      CHAR(36) NOT NULL,
  obs_dtm        DATETIME NOT NULL,
  val_num        DECIMAL(18,6) NOT NULL,
  quality_flag   VARCHAR(20) DEFAULT 'OK',
  ingest_batch_id CHAR(36),
  PRIMARY KEY (stream_id, obs_dtm, env_obs_uuid),
  INDEX ix_eo_time (obs_dtm),
  INDEX ix_eo_stream_time (stream_id, obs_dtm)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE COLUMNS (obs_dtm) (
  PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
  PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
  PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
  PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
  PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
  PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
  PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
  PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
  PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
  PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
  PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
  PARTITION p202612 VALUES LESS THAN ('2027-01-01'),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
);

-- 3.2.2 생육 관측(GROWTH_OBSERVATION)
CREATE TABLE IF NOT EXISTS growth_observation (
  growth_obs_uuid  CHAR(36) NOT NULL,
  cultivation_id   CHAR(36) NOT NULL,
  obs_dtm          DATETIME NOT NULL,
  indicator_type   VARCHAR(50) NOT NULL,
  val_num          DECIMAL(18,6),
  quality_flag     VARCHAR(20) DEFAULT 'OK',
  ingest_batch_id  CHAR(36),
  PRIMARY KEY (cultivation_id, obs_dtm, growth_obs_uuid),
  INDEX ix_go_time (obs_dtm),
  INDEX ix_go_cult_time (cultivation_id, obs_dtm),
  INDEX ix_go_indicator (indicator_type, obs_dtm)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE COLUMNS (obs_dtm) (
  PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
  PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
  PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
  PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
  PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
  PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
  PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
  PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
  PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
  PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
  PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
  PARTITION p202612 VALUES LESS THAN ('2027-01-01'),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
);

-- ============ 4.1 MCP 컨텍스트 패키지(표준/규칙 배포) ============

CREATE TABLE IF NOT EXISTS mcp_context_package (
  package_id    CHAR(36) PRIMARY KEY,
  package_name  VARCHAR(200) NOT NULL,
  package_ver   VARCHAR(50) NOT NULL,
  created_dtm   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_mcp_pkg (package_name, package_ver)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS mcp_context_item (
  item_id        CHAR(36) PRIMARY KEY,
  package_id     CHAR(36) NOT NULL,
  dataset_ver_id CHAR(36) NULL,
  item_type      VARCHAR(50) NOT NULL,
  item_key       VARCHAR(200) NOT NULL,
  content_ref    VARCHAR(1000) NOT NULL,
  created_dtm    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (package_id) REFERENCES mcp_context_package(package_id) ON DELETE CASCADE
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 4.2 문서/청크/임베딩(LLM 학습·검색 기반) ============

CREATE TABLE IF NOT EXISTS document (
  doc_id       CHAR(36) PRIMARY KEY,
  doc_type     VARCHAR(50) NOT NULL,
  title        VARCHAR(300),
  source_ref   VARCHAR(1000) NOT NULL,
  created_dtm  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doc_chunk (
  chunk_id     CHAR(36) PRIMARY KEY,
  doc_id       CHAR(36) NOT NULL,
  chunk_no     INT NOT NULL,
  text_ref     VARCHAR(1000) NOT NULL,
  token_count  INT,
  created_dtm  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (doc_id) REFERENCES document(doc_id) ON DELETE CASCADE,
  UNIQUE KEY uk_doc_chunk (doc_id, chunk_no)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS embedding_model (
  emb_model_id   CHAR(36) PRIMARY KEY,
  model_name     VARCHAR(200) NOT NULL,
  model_ver      VARCHAR(50) NOT NULL,
  dim            INT NOT NULL,
  created_dtm    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_emb_model (model_name, model_ver)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS vector_index (
  index_id      CHAR(36) PRIMARY KEY,
  index_name    VARCHAR(200) NOT NULL,
  index_ver     VARCHAR(50) NOT NULL,
  backend_type  VARCHAR(50) NOT NULL,
  created_dtm   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_vec_index (index_name, index_ver)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS embedding (
  emb_id        CHAR(36) PRIMARY KEY,
  chunk_id      CHAR(36) NOT NULL,
  emb_model_id  CHAR(36) NOT NULL,
  index_id      CHAR(36) NOT NULL,
  vector_ref    VARCHAR(1000) NOT NULL,
  created_dtm   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (chunk_id) REFERENCES doc_chunk(chunk_id) ON DELETE CASCADE,
  FOREIGN KEY (emb_model_id) REFERENCES embedding_model(emb_model_id),
  FOREIGN KEY (index_id) REFERENCES vector_index(index_id),
  UNIQUE KEY uk_chunk_model (chunk_id, emb_model_id)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 4.3 LLM 프롬프트/추론 로그(감사·재현성) ============

CREATE TABLE IF NOT EXISTS llm_prompt_template (
  template_id   CHAR(36) PRIMARY KEY,
  template_name VARCHAR(200) NOT NULL,
  template_ver  VARCHAR(50) NOT NULL,
  purpose       VARCHAR(200),
  prompt_ref    VARCHAR(1000) NOT NULL,
  created_dtm   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_prompt (template_name, template_ver)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS llm_inference_log (
  infer_id       CHAR(36) PRIMARY KEY,
  client_id      CHAR(36) NULL,
  template_id    CHAR(36) NOT NULL,
  package_id     CHAR(36) NULL,
  request_dtm    DATETIME NOT NULL,
  input_ref      VARCHAR(1000) NOT NULL,
  output_ref     VARCHAR(1000) NOT NULL,
  status         VARCHAR(30) NOT NULL,
  latency_ms     INT,
  created_dtm    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (template_id) REFERENCES llm_prompt_template(template_id),
  FOREIGN KEY (package_id) REFERENCES mcp_context_package(package_id)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 추가: RFP 부합성 추적 테이블 ============

-- 표준코드 저장소
CREATE TABLE IF NOT EXISTS standard_code (
  code_id        CHAR(36) PRIMARY KEY,
  code_name      VARCHAR(100) NOT NULL,
  code_desc      VARCHAR(500),
  parent_code_id CHAR(36),
  use_flag       CHAR(1) NOT NULL DEFAULT 'Y',
  applicable_to  VARCHAR(500),
  code_ver       VARCHAR(50),
  issued_dtm     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parent_code_id) REFERENCES standard_code(code_id)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 스키마 정의
CREATE TABLE IF NOT EXISTS schema_definition (
  schema_id       CHAR(36) PRIMARY KEY,
  table_name      VARCHAR(100) NOT NULL,
  column_name     VARCHAR(100) NOT NULL,
  data_type       VARCHAR(50) NOT NULL,
  data_length     INT,
  null_allowed    CHAR(1) DEFAULT 'Y',
  unit            VARCHAR(50),
  standard_code_ref CHAR(36),
  definition_ver  VARCHAR(50),
  created_dtm     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (standard_code_ref) REFERENCES standard_code(code_id),
  UNIQUE KEY uk_schema (table_name, column_name)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- RFP 완성도 점수 기록
CREATE TABLE IF NOT EXISTS package_completeness_score (
  score_id       CHAR(36) PRIMARY KEY,
  package_id     CHAR(36) NOT NULL,
  score_date     DATE NOT NULL,
  
  -- A. 표준코드 영역
  standard_codes_required   INT DEFAULT 8,
  standard_codes_populated  INT,
  standard_codes_validated  INT,
  score_a                   DECIMAL(5,2),
  
  -- B. 스키마 영역
  schema_required           INT DEFAULT 6,
  schema_populated          INT,
  schema_validated          INT,
  score_b                   DECIMAL(5,2),
  
  -- C. 품질규칙 영역
  quality_rules_required    INT DEFAULT 7,
  quality_rules_populated   INT,
  quality_rules_validated   INT,
  score_c                   DECIMAL(5,2),
  
  -- D. 데이터사전 영역
  data_dict_required        INT DEFAULT 7,
  data_dict_populated       INT,
  data_dict_validated       INT,
  score_d                   DECIMAL(5,2),
  
  -- 종합 점수
  package_score             DECIMAL(5,2),
  created_dtm               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (package_id) REFERENCES mcp_context_package(package_id),
  UNIQUE KEY uk_pkg_date (package_id, score_date)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 인덱스 최적화 ============

CREATE INDEX idx_env_obs_batch ON env_observation(ingest_batch_id);
CREATE INDEX idx_growth_obs_batch ON growth_observation(ingest_batch_id);
CREATE INDEX idx_mcp_item_type ON mcp_context_item(item_type);
CREATE INDEX idx_doc_type ON document(doc_type);
CREATE INDEX idx_doc_chunk_doc ON doc_chunk(doc_id);
CREATE INDEX idx_embedding_chunk ON embedding(chunk_id);
CREATE INDEX idx_embedding_model ON embedding(emb_model_id);
CREATE INDEX idx_llm_log_template ON llm_inference_log(template_id);
CREATE INDEX idx_llm_log_status ON llm_inference_log(status);
CREATE INDEX idx_llm_log_date ON llm_inference_log(request_dtm);
CREATE INDEX idx_standard_code_parent ON standard_code(parent_code_id);
CREATE INDEX idx_schema_table ON schema_definition(table_name);
CREATE INDEX idx_pkg_score_date ON package_completeness_score(score_date);
