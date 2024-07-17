package main

//go:generate gengo msg std_msgs/String
import (
	"testing"

	"github.com/dronedeploy/rosgo/libtest/libtest_publish_subscribe"
)

func main() {
	t := new(testing.T)
	libtest_publish_subscribe.RTTest(t)
}
