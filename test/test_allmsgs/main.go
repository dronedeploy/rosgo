package main

import (
	"testing"

	"github.com/dronedeploy/rosgo/libtest/libtest_allmsgs"
)

func main() {
	t := new(testing.T)
	libtest_allmsgs.RTTest(t)
}
