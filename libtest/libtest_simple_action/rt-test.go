package libtest_simple_action

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/team-rocos/rosgo/ros"
)

var feedback []int32

// ActionClient

// actionClient implements a simple action client
// using callbacks
type ActionClient struct {
	node ros.Node
	name string
	ac   ros.SimpleActionClient
}

// New Action client instantiates a new simple action client
func newActionClient(node ros.Node, name string, actionType ros.ActionType) (*ActionClient, error) {

	client, err := ros.NewSimpleActionClient(node, name, actionType)
	if err != nil {
		return nil, err
	}

	fc := &ActionClient{
		node: node,
		ac:   client,
	}

	fc.ac.WaitForServer(ros.NewDuration(0, 0))
	return fc, err
}

// Action Client active subscriber callback
func (fc *ActionClient) activeCb() {
}

// ActionClient feedback subscriber callback
func (fc *ActionClient) feedbackCb(fb *ros.DynamicMessage) {
	log.Printf("Received feedback: %v\n", fb)
	feedback = fb.Data()["sequence"].([]int32)
}

// ActionClient done subscriber callback
func (fc *ActionClient) doneCb(state uint8, result *ros.DynamicMessage) {
	log.Printf("DONE CALLBACK CALLED. RESULT: %v\n", result)
	fc.node.Shutdown()
}

// ActionClient send goal function used to send a goal to simple action server
func (fc *ActionClient) sendGoal(goal *ros.DynamicMessage) error {
	err := fc.ac.SendGoal(goal, fc.doneCb, fc.activeCb, fc.feedbackCb, "")
	if err != nil {
		return err
	}
	return nil
}

// ActionServer

// ActionServer implements a simple action server
// using the execute callback
type ActionServer struct {
	node   ros.Node
	as     ros.SimpleActionServer
	name   string
	fb     ros.Message
	result ros.Message
}

// newActionServer creates a new simple action server and starts it
func newActionServer(node ros.Node, name string, actionType ros.ActionType) {
	s := &ActionServer{
		node: node,
		name: name,
	}

	s.as = ros.NewSimpleActionServer(node, name,
		actionType, s.executeCallback, false)
	s.as.Start()
}

// executeCallback is the execution callback of the action server
func (s *ActionServer) executeCallback(goals interface{}, actionType interface{}) {

	fmt.Printf("Received in execute callback: %v\n", goals)
	goalMsg := goals.(*ros.DynamicMessage)

	// instantiate action message types
	feedType := actionType.(ros.ActionType).FeedbackType()
	resultType := actionType.(ros.ActionType).ResultType()

	// setup sequences
	feed := feedType.NewFeedbackMessage().(*ros.DynamicActionFeedback)

	seq := make([]int32, 0)
	seq = append(seq, 0)
	seq = append(seq, 1)
	feedMsg := feed.GetFeedback().(*ros.DynamicMessage)
	feedMsg.Data()["sequence"] = seq
	success := true

	// This method simply publishes feedback each second, incrementing count till goal acheived
	for i := 1; i < int(goalMsg.Data()["order"].(int32)); i++ {
		if s.as.IsPreemptRequested() {
			success = false
			if err := s.as.SetPreempted(nil, ""); err != nil {
				return
			}
			break
		}

		val := seq[i] + seq[i-1]
		seq = append(seq, val)
		feedMsg.Data()["sequence"] = seq

		s.as.PublishFeedback(feedMsg)
		time.Sleep(100 * time.Millisecond)
	}

	// Once goal achieved, publish result
	if success {
		result := resultType.NewResultMessage().(*ros.DynamicActionResult)
		resultMsg := result.GetResult().(*ros.DynamicMessage)
		resultMsg.Data()["sequence"] = seq
		if err := s.as.SetSucceeded(resultMsg, "goal"); err != nil {
			return
		}
	}
}

// Spin the server node in a separate thread
func spinServer(node ros.Node, quit <-chan struct{}) {

	//Initialize server
	defer node.Shutdown()
	for {
		select {
		case <-quit:
			node.Shutdown()
			return
		default:
			node.SpinOnce()
		}
	}
}

func RTTest(t *testing.T) {

	// Create a client node
	clientNode, err := ros.NewNode("test_fibonacci_client", os.Args)
	if err != nil {
		t.Errorf("could not create client node: %s", err)
		return
	}
	defer clientNode.Shutdown()

	// Create a server node
	serverNode, err := ros.NewNode("test_fibonacci_server", os.Args)
	if err != nil {
		t.Errorf("could not create server node: %s", err)
	}
	defer serverNode.Shutdown()

	// Create a dynamic action type
	actionType, err := ros.NewDynamicActionType("actionlib_tutorials/Fibonacci")
	if err != nil {
		t.Errorf("could not create action type: %s", err)
	}

	// Create a new action server
	newActionServer(serverNode, "fibonacci", actionType)

	// Spin server in another thread
	quitThread := make(chan struct{})
	go spinServer(serverNode, quitThread)

	// Create a goal message for the client
	goalMsg := actionType.GoalType().NewGoalMessage()
	goalRetrieved, err := goalMsg.GetGoal()
	if err != nil {
		t.Errorf("failed to get goal: %v", err)
	}

	goal := goalRetrieved.(*ros.DynamicMessage)
	goal.Data()["order"] = int32(5)

	// Create a client and send the goal to the server
	fc, err := newActionClient(clientNode, "fibonacci", actionType)
	if err != nil {
		t.Errorf("failed to create action client: %v", err)
	}
	err = fc.sendGoal(goal)
	if err != nil {
		t.Errorf("failed to send action goal: %v", err)
	}

	// Spin the client node
	for clientNode.OK() {

		_ = clientNode.SpinOnce()
	}
	quitThread <- struct{}{}
	// Our client ended because done was called
}
