package db

import (
	"database/sql"
	"fmt"
	"time"

	"obauditor/internal/logging"
)

// CleanupDao — удаление устаревших строк по достижении лимита.
//
// Стратегия:
//  1. COUNT(*) → если ≤ maxRows, выходим
//  2. targetRows = maxRows * 0.9 (запас, чтобы не запускать заново сразу)
//  3. boundary = SELECT id ... ORDER BY id ASC LIMIT 1 OFFSET (count - targetRows)
//  4. Цикл: DELETE WHERE id < boundary LIMIT chunkSize в своей транзакции
//     до тех пор, пока executeUpdate не вернёт 0.
//
// Зачем чанки: при штормовом сбое в таблице могут быть сотни тысяч лишних
// строк; один большой DELETE упрётся в ob_query_timeout и откатится.
type CleanupDao struct {
	db        *sql.DB
	log       *logging.Logger
	chunkSize int64
}

const defaultChunkSize int64 = 5000

func NewCleanupDao(db *sql.DB, log *logging.Logger, chunkSize int64) *CleanupDao {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	return &CleanupDao{db: db, log: log, chunkSize: chunkSize}
}

// CleanDdlDclAuditLog удаляет старые строки из ddl_dcl_audit_log.
func (c *CleanupDao) CleanDdlDclAuditLog(maxRows int64) (int64, error) {
	return c.cleanTable("admintools.ddl_dcl_audit_log", maxRows)
}

// CleanSessions удаляет старые строки из sessions.
func (c *CleanupDao) CleanSessions(maxRows int64) (int64, error) {
	return c.cleanTable("admintools.sessions", maxRows)
}

func (c *CleanupDao) cleanTable(tableName string, maxRows int64) (int64, error) {
	if maxRows <= 0 {
		c.log.Debugf("[CleanupDao] %s — maxRows=0, cleanup skipped", tableName)
		return 0, nil
	}

	var count int64
	if err := c.db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s: %w", tableName, err)
	}
	if count <= maxRows {
		c.log.Debugf("[CleanupDao] %s — count=%d <= maxRows=%d, skip", tableName, count, maxRows)
		return 0, nil
	}

	// Оставляем 90% от лимита — следующая очистка не понадобится сразу же
	targetRows := int64(float64(maxRows) * 0.9)
	offset := count - targetRows

	var boundary int64
	offsetSQL := "SELECT id FROM " + tableName + " ORDER BY id ASC LIMIT 1 OFFSET ?"
	err := c.db.QueryRow(offsetSQL, offset).Scan(&boundary)
	if err == sql.ErrNoRows {
		c.log.Debugf("[CleanupDao] %s — boundary not found, skip", tableName)
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("find boundary in %s: %w", tableName, err)
	}

	c.log.Debugf("[CleanupDao] %s — start chunked delete, id < %d (count=%d, maxRows=%d, target=%d, needToDelete=%d, chunkSize=%d)",
		tableName, boundary, count, maxRows, targetRows, offset, c.chunkSize)

	t0 := time.Now()
	var totalDeleted int64
	var chunkNum int

	deleteSQL := "DELETE FROM " + tableName + " WHERE id < ? LIMIT ?"

	for {
		// Каждый чанк — отдельная транзакция
		tx, err := c.db.Begin()
		if err != nil {
			if totalDeleted > 0 {
				c.log.Infof("[CleanupDao] %s — failed after %d chunk(s), %d row(s) deleted, error: %v",
					tableName, chunkNum, totalDeleted, err)
			}
			return totalDeleted, fmt.Errorf("begin tx: %w", err)
		}

		res, err := tx.Exec(deleteSQL, boundary, c.chunkSize)
		if err != nil {
			_ = tx.Rollback()
			if totalDeleted > 0 {
				c.log.Infof("[CleanupDao] %s — failed after %d chunk(s), %d row(s) deleted, error: %v",
					tableName, chunkNum, totalDeleted, err)
			}
			return totalDeleted, fmt.Errorf("delete chunk: %w", err)
		}
		chunk, _ := res.RowsAffected()

		if err := tx.Commit(); err != nil {
			return totalDeleted, fmt.Errorf("commit chunk: %w", err)
		}

		if chunk == 0 {
			break
		}
		chunkNum++
		totalDeleted += chunk
		c.log.Debugf("[CleanupDao] %s — chunk #%d deleted %d row(s), total %d",
			tableName, chunkNum, chunk, totalDeleted)
	}

	elapsedMs := time.Since(t0).Milliseconds()
	c.log.Infof("[CleanupDao] %s — deleted %d old row(s) in %d chunk(s), %d ms (kept ~%d, target=%d)",
		tableName, totalDeleted, chunkNum, elapsedMs, count-totalDeleted, targetRows)
	return totalDeleted, nil
}
