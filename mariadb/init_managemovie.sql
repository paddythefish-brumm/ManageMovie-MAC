CREATE TABLE IF NOT EXISTS app_state (
  state_key VARCHAR(128) NOT NULL PRIMARY KEY,
  state_value TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS tmdb_cache (
  prompt_hash CHAR(64) NOT NULL PRIMARY KEY,
  prompt_preview VARCHAR(255) NOT NULL,
  prompt_text MEDIUMTEXT NOT NULL,
  model VARCHAR(128) NOT NULL,
  rows_json LONGTEXT NOT NULL,
  rows_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_used_at TIMESTAMP NULL DEFAULT NULL,
  expires_at TIMESTAMP NOT NULL,
  KEY idx_tmdb_cache_expires (expires_at),
  KEY idx_tmdb_cache_last_used (last_used_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS tmdb_cache_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  prompt_hash CHAR(64) NOT NULL,
  prompt_preview VARCHAR(255) NOT NULL,
  prompt_text MEDIUMTEXT NOT NULL,
  model VARCHAR(128) NOT NULL,
  rows_json LONGTEXT NOT NULL,
  rows_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NOT NULL,
  KEY idx_tmdb_cache_history_prompt (prompt_hash),
  KEY idx_tmdb_cache_history_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
