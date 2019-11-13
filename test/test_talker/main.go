package main

//go:generate gengo msg std_msgs/String
import (
	"fmt"
	"github.com/edwinhayes/rosgo/ros"
	"os"
	"rosgo_tests"
	"std_msgs"
	"time"
)

func main() {
	node, err := ros.NewNode("/talker", os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer node.Shutdown()
	node.Logger().SetSeverity(ros.LogLevelDebug)
	pub := node.NewPublisher("/chatter", std_msgs.MsgString)
	pub2 := node.NewPublisher("/all", rosgo_tests.MsgAllFieldTypes)

	for node.OK() {
		node.SpinOnce()
		var msg std_msgs.String
		msg.Data = fmt.Sprintf("hello %s", time.Now().String())
		fmt.Println(msg.Data)
		pub.Publish(&msg)
		var m rosgo_tests.AllFieldTypes
		pub2.Publish(&m)
		time.Sleep(time.Second)
	}
}