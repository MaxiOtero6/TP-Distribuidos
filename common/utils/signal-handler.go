package utils

import "errors"

var ErrSignalReceived = errors.New("signal received")

func IsDone(done <-chan bool) bool {
	select {
	case <-done:
		return true
	default:
	}
	return false
}
