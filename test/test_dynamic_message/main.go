package main

import (
	"testing"

	"github.com/dronedeploy/rosgo/libtest/libtest_dynamic_message"
)

func main() {
	t := new(testing.T)
	libtest_dynamic_message.RTTest(t)
}
