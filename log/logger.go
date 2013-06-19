// a very thin convenience wrapper over go's log package
// that also alters the semantics of go log's Error and Fatal:
// none of the functions of this package will ever panic() or
// call os.Exit().
//
// Log levels are prefixed to the user log data for each
// eponymous function i.e. logger.Error(..) will emit a log
// message that begins (after prefix) with ERROR - ..
//
// Package also opts for logger.Lmicroseconds prefix in addition
// to the default Prefix().
package log

import (
	"fmt"
	"io"
	stdlib "log"
    "runtime"
    "strings"
    "strconv"
)

const (
	delim  = " - "
	FATAL  = "[FATAL]"
	ERROR  = "[ERROR]"
	WARN   = "[WARN ]"
	DEBUG  = "[DEBUG]"
	INFO   = "[INFO ]"
	levlen = len(INFO)
	len2   = len(delim) + levlen
	len3   = 2*len(delim) + levlen
)

const (
	LEVEL_FATAL = iota
	LEVEL_ERROR
	LEVEL_WARN
	LEVEL_INFO
	LEVEL_DEBUG
)

var levels = []string{FATAL, ERROR, WARN, DEBUG, INFO}

type SysLog struct {
	logger *stdlib.Logger
    flag int
}

func (sl *SysLog) Flags() int {
	return sl.flag
}

func NewSysLog(out io.Writer, prefix string, flag int) *SysLog {
	return &SysLog{stdlib.New(out, prefix, 3), flag}
}

// NOTE - the semantics here are different from go's logger.Fatal
// It will neither panic nor exit
func (sl *SysLog) Fatal(meta string, e error) {
	if sl.Flags() >= LEVEL_FATAL {
		sl.logger.Println(join3(FATAL, meta, e.Error()))
	}
}

// NOTE - the semantics here are different from go's logger.Fatal
// It will neither panic nor exit
func (sl *SysLog) Fatalf(v ...interface{}) {
	if sl.Flags() >= LEVEL_FATAL {
		sl.logger.Println(join2(FATAL, fmt.Sprint(v...)))
	}
}

func (sl *SysLog) Error(meta string, e error) {
	if sl.Flags() >= LEVEL_ERROR {
		sl.logger.Println(join3(ERROR, meta, e.Error()))
	}
}
func (sl *SysLog) Errorf(v ...interface{}) {
	if sl.Flags() >= LEVEL_ERROR {
		sl.logger.Println(join2(ERROR, fmt.Sprint(v...)))
	}
}

func (sl *SysLog) Debug(msg string) {
	if sl.Flags() >= LEVEL_DEBUG {
		sl.logger.Println(join2(DEBUG, msg))
	}
}

func (sl *SysLog) Debugf(v ...interface{}) {
	if sl.Flags() >= LEVEL_DEBUG {
		sl.logger.Println(join2(DEBUG, fmt.Sprint(v...)))
	}
}

func (sl *SysLog) Warn(msg string) {
	if sl.Flags() >= LEVEL_WARN {
		sl.logger.Println(join2(WARN, msg))
	}
}

func (sl *SysLog) Warnf(v ...interface{}) {
	if sl.Flags() >= LEVEL_WARN {
		sl.logger.Println(join2(WARN, fmt.Sprint(v...)))
	}
}

func (sl *SysLog) Info(msg string) {
	if sl.Flags() >= LEVEL_INFO {
		sl.logger.Println(join2(INFO, msg))
	}
}

func (sl *SysLog) Infof(v ...interface{}) {
	if sl.Flags() >= LEVEL_INFO {
		sl.logger.Println(join2(INFO, fmt.Sprint(v...)))
	}
}

func join2(level, msg string) string {
    l := GetFileInfo()
	n := len(msg) + len2 + len(l)
	j := make([]byte, n)
	o := copy(j, level)
    o += copy(j[o:], l)
	o += copy(j[o:], delim)
	copy(j[o:], msg)
	return string(j)
}
func join3(level, meta, msg string) string {
	n := len(meta) + len(msg) + len3
	j := make([]byte, n)
	o := copy(j, level)
	o += copy(j[o:], delim)
	o += copy(j[o:], meta)
	o += copy(j[o:], delim)
	copy(j[o:], msg)
	return string(j)
}

func GetFileInfo() string {
    pc, file, line, _ := runtime.Caller(3)
    fileName := strings.Split(file, "/")
    val := fileName[len(fileName)-2] + "/" + fileName[len(fileName)-1]
    return " " + val + ":" + strconv.Itoa(line) + ":" + runtime.FuncForPC(pc).Name()
}

