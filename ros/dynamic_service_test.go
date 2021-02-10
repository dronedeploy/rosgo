package ros

import (
	"testing"
)

func TestDynamicService_ServiceType_Load(t *testing.T) {
	serviceType, err := NewDynamicServiceType("turtlesim/TeleportRelative")

	if err != nil {
		t.Skipf("test skipped because ROS environment not set up, err: %s", err)
		return
	}

	checkIfValidDynamicMessageType(t, serviceType.reqType, "request")
	checkIfValidDynamicMessageType(t, serviceType.resType, "response")
}

// Helper classes
func checkIfValidDynamicMessageType(t *testing.T, msgType MessageType, name string) {
	dynMsgType, ok := msgType.(*DynamicMessageType)

	if !ok {
		t.Fatalf("expected %s to be dynamic message type", name)
	}

	if dynMsgType.nested == nil {
		t.Fatalf("%s type nested is nil!", name)
	}

	if dynMsgType.spec == nil {
		t.Fatalf("%s type spec is nil!", name)
	}

	if dynMsgType.spec.Fields == nil {
		t.Fatalf("%s type Fields is nil!", name)
	}
}
