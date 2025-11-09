-- =========================================================
-- 🔹 GRUPOS DO TELEGRAM
-- =========================================================
CREATE TABLE groups (
                        id BIGINT PRIMARY KEY,                  -- chat_id do Telegram
                        title VARCHAR(255) NOT NULL,
                        username VARCHAR(255),
                        last_update_id BIGINT DEFAULT 0,        -- controle de ingestão
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- =========================================================
-- 🔹 CREDENCIAIS (pode ter múltiplas contas/API)
-- =========================================================
CREATE TABLE credentials (
                             id INT AUTO_INCREMENT PRIMARY KEY,
                             label VARCHAR(100) NOT NULL,
                             api_id VARCHAR(100) NOT NULL,
                             api_hash VARCHAR(255) NOT NULL,
                             phone VARCHAR(50),
                             session_name VARCHAR(100),
                             active BOOLEAN DEFAULT TRUE,
                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- 🔹 RELAÇÃO GRUPO ↔ CREDENCIAL
-- =========================================================
CREATE TABLE group_credentials (
                                   id INT AUTO_INCREMENT PRIMARY KEY,
                                   group_id BIGINT NOT NULL,
                                   credential_id INT NOT NULL,
                                   FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                                   FOREIGN KEY (credential_id) REFERENCES credentials(id) ON DELETE CASCADE
);

-- =========================================================
-- 🔹 MODELOS (pessoas / performers)
-- =========================================================
CREATE TABLE models (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        stage_name VARCHAR(255) NOT NULL,
                        full_name VARCHAR(255),
                        aliases JSON DEFAULT (JSON_ARRAY()),      -- lista de nomes alternativos
                        reference_path TEXT,                      -- caminho base no Spaces
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE (stage_name)
);

-- =========================================================
-- 🔹 MÍDIAS
-- =========================================================
CREATE TABLE media (
                       id BIGINT AUTO_INCREMENT PRIMARY KEY,
                       telegram_message_id BIGINT NOT NULL,
                       group_id BIGINT NOT NULL,
                       telegram_file_id VARCHAR(255),
                       sha256_hex CHAR(64) NOT NULL,             -- hash exato
                       phash BIGINT,                             -- perceptual hash
                       mime VARCHAR(100),
                       width INT,
                       height INT,
                       size_bytes BIGINT,
                       local_path TEXT,
                       remote_url TEXT,
                       storage_key TEXT,
                       state ENUM('pending_classification','classified','uploaded','needs_review','approved','error')
                                            DEFAULT 'pending_classification',
                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                       UNIQUE (sha256_hex),
                       FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE INDEX idx_media_group_state ON media(group_id, state);
CREATE INDEX idx_media_phash ON media(phash);

-- =========================================================
-- 🔹 CLASSIFICAÇÕES (IA ou humana)
-- =========================================================
CREATE TABLE media_classifications (
                                       id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                       media_id BIGINT NOT NULL,
                                       model_id INT NULL,
                                       label VARCHAR(255),
                                       confidence DECIMAL(5,4),
                                       source VARCHAR(50) NOT NULL,              -- ex: openai-vision, clip, etc.
                                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                       FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
                                       FOREIGN KEY (model_id) REFERENCES models(id)
);

CREATE INDEX idx_classifications_media ON media_classifications(media_id);

-- =========================================================
-- 🔹 APROVAÇÕES MANUAIS
-- =========================================================
CREATE TABLE media_approvals (
                                 id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                 media_id BIGINT NOT NULL,
                                 approved BOOLEAN DEFAULT FALSE,
                                 reviewed_by VARCHAR(100),
                                 comment TEXT,
                                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                 FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

-- =========================================================
-- 🔹 CUSTOS (por etapa)
-- =========================================================
CREATE TABLE costs (
                       id BIGINT AUTO_INCREMENT PRIMARY KEY,
                       media_id BIGINT,
                       group_id BIGINT,
                       model_id INT,
                       step ENUM('ingest','classify','upload','notify') NOT NULL,
                       currency VARCHAR(10) DEFAULT 'USD',
                       amount DECIMAL(12,6) NOT NULL,
                       meta JSON DEFAULT (JSON_OBJECT()),
                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
                       FOREIGN KEY (group_id) REFERENCES groups(id),
                       FOREIGN KEY (model_id) REFERENCES models(id)
);

CREATE INDEX idx_costs_media ON costs(media_id);

-- =========================================================
-- 🔹 LOGS DE ERRO E SUCESSO
-- =========================================================
CREATE TABLE process_logs (
                              id BIGINT AUTO_INCREMENT PRIMARY KEY,
                              media_id BIGINT,
                              group_id BIGINT,
                              step VARCHAR(50),
                              status ENUM('success','error') DEFAULT 'success',
                              message TEXT,
                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
                              FOREIGN KEY (group_id) REFERENCES groups(id)
);
