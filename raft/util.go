package raft

import (
	"log"
	"math/rand"
	"os"
)

type Logger struct {
	debug bool
	*log.Logger
}

var logger = NewLogger(false)

func NewLogger(debug bool) *Logger {
	return &Logger{
		debug,
		log.New(os.Stderr, `[Raft]`, log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}

func (l *Logger) Printf(format string, v ...interface{}) {
	if l.debug {
		l.Logger.Printf(format, v...)
	}
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func RandomID(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	var result []byte

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

type UintSlice []uint64

func (u UintSlice) Len() int {
	return len(u)
}

func (u UintSlice) Less(i, j int) bool {
	return u[i] < u[j]
}

func (u UintSlice) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}
