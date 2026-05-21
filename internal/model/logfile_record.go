package model

import (
	"fmt"
	"path/filepath"
)

// LogFileRecord — POJO строки таблицы logfiles.
// Хранит состояние обработки одного лог-файла.
//
// CollectorId — идентификатор сервиса который читает этот файл.
//               Входит в UNIQUE KEY вместе с file_dir и file_name.
//
// FileIp — IP узла-источника:
//   SERVER: из первой строки файла (address: "IP:port")
//   PROXY:  из строки "server session born" (local_ip:{IP:port})
//
// LastLineNum — байтовый OFFSET (не номер строки!).
type LogFileRecord struct {
	Id            int64
	CollectorId   string
	FileDir       string
	FileName      string
	FileType      string // "SERVER" или "PROXY"
	FileSize      int64
	LastLineNum   int64  // байтовый offset
	LastTimestamp string // может быть пустой
	LastTid       *int   // может быть nil
	LastTraceId   string // может быть пустой
	FileIp        string // может быть пустой
}

func (r *LogFileRecord) FullPath() string {
	return filepath.Join(r.FileDir, r.FileName)
}

func (r *LogFileRecord) String() string {
	return fmt.Sprintf(
		"LogFileRecord{collector='%s', dir='%s', name='%s', type=%s, size=%d, offset=%d, ip='%s', lastTs='%s'}",
		r.CollectorId, r.FileDir, r.FileName, r.FileType,
		r.FileSize, r.LastLineNum, r.FileIp, r.LastTimestamp)
}
