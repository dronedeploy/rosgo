package main

//go:generate gengo msg std_msgs/String
import (
	"github.com/edwinhayes/rosgo/libtest/libtest_talker_with_callbacks"
	"testing"
)

func main() {
	t := new(testing.T)
	libtest_talker_with_callbacks.RTTest(t)
}
