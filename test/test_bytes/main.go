package main

import (
	"testing"

	"github.com/dronedeploy/rosgo/libtest/libtest_bytes"
)

func main() {
	t := new(testing.T)
	libtest_bytes.RTTest(t)
}
