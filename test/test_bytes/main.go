package main

import (
	"testing"

	"github.com/team-rocos/rosgo/libtest/libtest_bytes"
)

func main() {
	t := new(testing.T)
	libtest_bytes.RTTest(t)
}
