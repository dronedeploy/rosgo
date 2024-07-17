package main

import (
	"testing"

	"github.com/dronedeploy/rosgo/libtest/libtest_param"
)

func main() {
	t := new(testing.T)
	libtest_param.RTTest(t)
}
