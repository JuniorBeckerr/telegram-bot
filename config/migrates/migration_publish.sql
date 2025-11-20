-- =====================================================
-- MIGRATION: Sistema de Publicação de Mídias
-- Data: 2024
-- Descrição: Adiciona suporte a grupos próprios e publicação
-- =====================================================

-- Transação para garantir atomicidade
BEGIN TRANSACTION;

-- =====================================================
-- 1. ALTERAÇÕES NA TABELA GROUPS
-- =====================================================

-- Adiciona campo para identificar grupos próprios
ALTER TABLE groups ADD COLUMN is_owner BOOLEAN DEFAULT 0;

-- Adiciona campos de configuração de publicação
ALTER TABLE groups ADD COLUMN publish_enabled BOOLEAN DEFAULT 0;
ALTER TABLE groups ADD COLUMN publish_interval_minutes INTEGER DEFAULT 60;
ALTER TABLE groups ADD COLUMN last_publish_at TIMESTAMP NULL;

-- Índices para otimização
CREATE INDEX IF NOT EXISTS idx_groups_is_owner ON groups(is_owner);
CREATE INDEX IF NOT EXISTS idx_groups_publish_enabled ON groups(publish_enabled);

-- =====================================================
-- 2. TABELA BOTS
-- =====================================================

CREATE TABLE IF NOT EXISTS bots (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    name VARCHAR(255) NOT NULL,
    token VARCHAR(255) NOT NULL UNIQUE,
    username VARCHAR(255),
    description TEXT,
    active BOOLEAN DEFAULT 1,
    last_used_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE INDEX IF NOT EXISTS idx_bots_active ON bots(active);
CREATE INDEX IF NOT EXISTS idx_bots_username ON bots(username);

-- =====================================================
-- 3. TABELA GROUP_BOTS
-- =====================================================

CREATE TABLE IF NOT EXISTS group_bots (
                                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                                          group_id BIGINT NOT NULL,
                                          bot_id INTEGER NOT NULL,
                                          is_publisher BOOLEAN DEFAULT 1,
                                          is_admin BOOLEAN DEFAULT 0,
                                          permissions TEXT DEFAULT NULL,
                                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                                          FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY (bot_id) REFERENCES bots(id) ON DELETE CASCADE,
    UNIQUE(group_id, bot_id)
    );

CREATE INDEX IF NOT EXISTS idx_group_bots_group ON group_bots(group_id);
CREATE INDEX IF NOT EXISTS idx_group_bots_bot ON group_bots(bot_id);
CREATE INDEX IF NOT EXISTS idx_group_bots_publisher ON group_bots(is_publisher);

-- =====================================================
-- 4. TABELA GROUP_PUBLISH
-- =====================================================

CREATE TABLE IF NOT EXISTS group_publish (
                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                             group_id BIGINT NOT NULL,
                                             media_id INTEGER NOT NULL,
                                             bot_id INTEGER NOT NULL,
                                             telegram_message_id BIGINT NULL,
                                             file_id VARCHAR(255) NULL,
    status VARCHAR(50) DEFAULT 'pending',
    caption TEXT NULL,
    published_at TIMESTAMP NULL,
    error_message TEXT NULL,
    retry_count INTEGER DEFAULT 0,
    metadata TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (bot_id) REFERENCES bots(id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS idx_group_publish_group ON group_publish(group_id);
CREATE INDEX IF NOT EXISTS idx_group_publish_media ON group_publish(media_id);
CREATE INDEX IF NOT EXISTS idx_group_publish_bot ON group_publish(bot_id);
CREATE INDEX IF NOT EXISTS idx_group_publish_status ON group_publish(status);
CREATE INDEX IF NOT EXISTS idx_group_publish_published_at ON group_publish(published_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_group_publish_unique ON group_publish(group_id, media_id);

-- =====================================================
-- 5. TABELA PUBLISH_QUEUE
-- =====================================================

CREATE TABLE IF NOT EXISTS publish_queue (
                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                             group_id BIGINT NOT NULL,
                                             media_id INTEGER NOT NULL,
                                             bot_id INTEGER NULL,
                                             priority INTEGER DEFAULT 0,
                                             scheduled_at TIMESTAMP NULL,
                                             attempts INTEGER DEFAULT 0,
                                             max_attempts INTEGER DEFAULT 3,
                                             status VARCHAR(50) DEFAULT 'pending',
    last_attempt_at TIMESTAMP NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (bot_id) REFERENCES bots(id) ON DELETE SET NULL
    );

CREATE INDEX IF NOT EXISTS idx_publish_queue_status ON publish_queue(status);
CREATE INDEX IF NOT EXISTS idx_publish_queue_scheduled ON publish_queue(scheduled_at);
CREATE INDEX IF NOT EXISTS idx_publish_queue_priority ON publish_queue(priority DESC);
CREATE INDEX IF NOT EXISTS idx_publish_queue_group ON publish_queue(group_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_queue_unique ON publish_queue(group_id, media_id);

-- =====================================================
-- 6. TABELA PUBLISH_RULES
-- =====================================================

CREATE TABLE IF NOT EXISTS publish_rules (
                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                             group_id BIGINT NOT NULL,
                                             source_group_id BIGINT NULL,
                                             name VARCHAR(255) NULL,
    classification_filter TEXT NULL,
    min_approval_score FLOAT DEFAULT 0,
    approval_required BOOLEAN DEFAULT 1,
    auto_publish BOOLEAN DEFAULT 0,
    daily_limit INTEGER NULL,
    hourly_limit INTEGER NULL,
    caption_template TEXT NULL,
    watermark_enabled BOOLEAN DEFAULT 0,
    watermark_text VARCHAR(255) NULL,
    priority INTEGER DEFAULT 0,
    active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY (source_group_id) REFERENCES groups(id) ON DELETE SET NULL
    );

CREATE INDEX IF NOT EXISTS idx_publish_rules_group ON publish_rules(group_id);
CREATE INDEX IF NOT EXISTS idx_publish_rules_source ON publish_rules(source_group_id);
CREATE INDEX IF NOT EXISTS idx_publish_rules_active ON publish_rules(active);

-- =====================================================
-- 7. TABELA PUBLISH_STATS (Opcional - para métricas)
-- =====================================================

CREATE TABLE IF NOT EXISTS publish_stats (
                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                             group_id BIGINT NOT NULL,
                                             date DATE NOT NULL,
                                             total_published INTEGER DEFAULT 0,
                                             total_failed INTEGER DEFAULT 0,
                                             total_pending INTEGER DEFAULT 0,
                                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                             updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                                             FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    UNIQUE(group_id, date)
    );

CREATE INDEX IF NOT EXISTS idx_publish_stats_group ON publish_stats(group_id);
CREATE INDEX IF NOT EXISTS idx_publish_stats_date ON publish_stats(date);

-- =====================================================
-- 8. TABELA PUBLISH_LOG (Para auditoria)
-- =====================================================

CREATE TABLE IF NOT EXISTS publish_log (
                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           group_publish_id INTEGER NULL,
                                           action VARCHAR(50) NOT NULL,
    details TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (group_publish_id) REFERENCES group_publish(id) ON DELETE SET NULL
    );

CREATE INDEX IF NOT EXISTS idx_publish_log_action ON publish_log(action);
CREATE INDEX IF NOT EXISTS idx_publish_log_created ON publish_log(created_at);

-- =====================================================
-- COMMIT
-- =====================================================

COMMIT;

-- =====================================================
-- VERIFICAÇÃO
-- =====================================================

-- Lista todas as tabelas criadas
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;


ALTER TABLE publish_rules ADD COLUMN random_model BOOLEAN DEFAULT 0;


ALTER TABLE publish_rules ADD COLUMN batch_size INTEGER DEFAULT 6;
