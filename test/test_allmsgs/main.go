package main

import (
	"testing"

	"github.com/team-rocos/rosgo/libtest/libtest_allmsgs"
)

func main() {
	t := new(testing.T)
	libtest_allmsgs.RTTest(t)
}
