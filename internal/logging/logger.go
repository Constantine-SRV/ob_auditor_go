// Package logging — простой logger с уровнями (DEBUG/INFO/ERROR).
//
// ERROR — всегда в stderr.
// INFO  — печатается при INFO и DEBUG.
// DEBUG — только при DEBUG.
package logging

import (
	"fmt"
	"os"

	"obauditor/internal/config"
)

// Logger привязан к уровню из конфига.
type Logger struct {
	Level config.LogLevel
}

func New(level config.LogLevel) *Logger {
	return &Logger{Level: level}
}

func (l *Logger) IsDebug() bool { return l.Level >= config.LevelDebug }
func (l *Logger) IsInfo() bool  { return l.Level >= config.LevelInfo }

// Debugf — только при DEBUG.
func (l *Logger) Debugf(format string, args ...any) {
	if l.Level >= config.LevelDebug {
		if format != "" && format[len(format)-1] != '\n' {
			format += "\n"
		}
		fmt.Printf(format, args...)
	}
}

// Infof — при INFO и DEBUG.
func (l *Logger) Infof(format string, args ...any) {
	if l.Level >= config.LevelInfo {
		if format != "" && format[len(format)-1] != '\n' {
			format += "\n"
		}
		fmt.Printf(format, args...)
	}
}

// Errorf — всегда в stderr.
func (l *Logger) Errorf(format string, args ...any) {
	if format != "" && format[len(format)-1] != '\n' {
		format += "\n"
	}
	fmt.Fprintf(os.Stderr, format, args...)
}
