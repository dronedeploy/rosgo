package ros

// IMPORT REQUIRED PACKAGES.

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/team-rocos/rosgo/libgengo"
)

// DEFINE PUBLIC STRUCTURES.

// DynamicActionType abstracts the schema of a ROS Action whose schema is only known at runtime.
// DynamicActionType implements the rosgo actionlib.ActionType interface, allowing it to be used throughout rosgo in the same manner as action schemas generated
// at compiletime by gengo.
type DynamicActionType struct {
	name         string
	md5sum       string
	text         string
	goalType     ActionGoalType
	feedbackType ActionFeedbackType
	resultType   ActionResultType
}

// DynamicAction abstracts an instance of a ROS Action whose type is only known at runtime.  The schema of the action is denoted by the referenced DynamicActionType, while the
// Goal, Feedback and Result references the rosgo Messages it implements.  DynamicAction implements the rosgo actionlib.Action interface, allowing
// it to be used throughout rosgo in the same manner as action types generated at compiletime by gengo.
type DynamicAction struct {
	dynamicType *DynamicActionType
	Goal        ActionGoal
	Feedback    ActionFeedback
	Result      ActionResult
}

// DynamicAction* interfaces.
type DynamicActionGoal struct{ DynamicMessage }
type DynamicActionFeedback struct{ DynamicMessage }
type DynamicActionResult struct{ DynamicMessage }

// DynamicActionType* type interfaces
type DynamicActionGoalType struct{ DynamicMessageType }
type DynamicActionFeedbackType struct{ DynamicMessageType }
type DynamicActionResultType struct{ DynamicMessageType }

// DynamicAction** shared interfaces.
type DynamicActionGoalID struct{ DynamicMessage }
type DynamicActionStatus struct{ DynamicMessage }
type DynamicActionStatusArray struct{ DynamicMessage }

// DynamicActionType** shared type interfaces
type DynamicActionGoalIDType struct{ DynamicMessageType }
type DynamicActionStatusType struct{ DynamicMessageType }
type DynamicActionStatusArrayType struct{ DynamicMessageType }

// DEFINE PRIVATE STRUCTURES.

// DEFINE PUBLIC GLOBALS.

// DEFINE PRIVATE GLOBALS.

// NewDynamicActionType generates a DynamicActionType corresponding to the specified typeName from the available ROS action definitions; typeName should be a fully-qualified
// ROS action type name.  The first time the function is run, a message/service/action 'context' is created by searching through the available ROS definitions, then the ROS action to
// be used for the definition is looked up by name.  On subsequent calls, the ROS action type is looked up directly from the existing context.
func NewDynamicActionType(typeName string) (*DynamicActionType, error) {
	return newDynamicActionTypeNested(typeName, "")
}

// newDynamicActionTypeNested generates a DynamicActionType from the available ROS definitions.  The first time the function is run, a message/service/action 'context' is created by
// searching through the available ROS definitions, then the ROS action type to use for the defintion is looked up by name.  On subsequent calls, the ROS action type
// is looked up directly from the existing context.  This 'nested' version of the function is able to be called recursively, where packageName should be the typeName of the
// parent ROS action; this is used internally for handling complex ROS action.
func newDynamicActionTypeNested(typeName string, packageName string) (*DynamicActionType, error) {
	// Create an empty action type.
	m := new(DynamicActionType)

	if context == nil {
		// Create context for our ROS install.
		c, err := libgengo.NewPkgContext(strings.Split(GetRuntimePackagePath(), ":"))
		if err != nil {
			return nil, err
		}
		context = c
	}

	// We need to try to look up the full name, in case we've just been given a short name.
	fullname := typeName
	_, ok := context.GetActions()[fullname]
	if !ok {
		// Messages in the same package are allowed to use relative names, so try prefixing the package.
		if packageName != "" {
			fullname = packageName + "/" + fullname
		}
	}

	// Load context for the target message.
	spec, err := context.LoadAction(fullname)
	if err != nil {
		return nil, err
	}

	// Now we know all about the service!
	m.name = spec.ShortName
	m.md5sum = spec.MD5Sum
	m.text = spec.Text

	// Create Dynamic Goal Type
	goalType, err := NewDynamicMessageTypeFromSpec(spec.ActionGoal)
	if err != nil {
		return nil, err
	}
	m.goalType = &DynamicActionGoalType{*goalType}

	// Create Dynamic Feedback Type
	feedbackType, err := NewDynamicMessageTypeFromSpec(spec.ActionFeedback)
	if err != nil {
		return nil, err
	}
	m.feedbackType = &DynamicActionFeedbackType{*feedbackType}

	// Create Dynamic Result Type
	resultType, err := NewDynamicMessageTypeFromSpec(spec.ActionResult)
	if err != nil {
		return nil, err
	}
	m.resultType = &DynamicActionResultType{*resultType}

	// We've successfully made a new service type matching the requested ROS type.
	return m, nil

}

// DEFINE PUBLIC RECEIVER FUNCTIONS.

// DynamicActionGoalID Type and New Message instantiators
func NewActionGoalIDType() ActionGoalIDType {
	goalid := DynamicActionGoalIDType{}
	goalType, _ := NewDynamicMessageTypeLiteral("actionlib_msgs/GoalID")
	goalid.DynamicMessageType = goalType
	return &goalid
}
func (a *DynamicActionGoalIDType) NewGoalIDMessage() ActionGoalID {
	m := DynamicActionGoalID{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// DynamicActionStatus Type and New Message instantiators
func NewActionStatusType() ActionStatusType {
	status := DynamicActionStatusType{}
	statusType, _ := NewDynamicMessageTypeLiteral("actionlib_msgs/GoalStatus")
	status.DynamicMessageType = statusType
	return &status
}
func (a *DynamicActionStatusType) NewStatusMessage() ActionStatus {
	m := DynamicActionStatus{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// DynamicActionStatusArray Type and New Message instantiators
func NewActionStatusArrayType() ActionStatusArrayType {
	statusArray := DynamicActionStatusArrayType{}
	statusArrayType, _ := NewDynamicMessageTypeLiteral("actionlib_msgs/GoalStatusArray")
	statusArray.DynamicMessageType = statusArrayType
	return &statusArray
}
func (a *DynamicActionStatusArrayType) NewStatusArrayMessage() ActionStatusArray {
	m := DynamicActionStatusArray{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// NewStatusArrayFromInterface creates an ActionStatusArray provided an interface.
// Used where serialization/deserialization removes traces of action interface types from a message
func (a *DynamicActionStatusArrayType) NewStatusArrayFromInterface(statusArr interface{}) ActionStatusArray {
	statusArray := statusArr.(*DynamicMessage)
	status := a.NewStatusArrayMessage().(*DynamicActionStatusArray)
	statusList := make([]ActionStatus, 0)

	if statusMsgsInterface, ok := statusArray.Data()["status_list"]; ok {
		if statusMsgs, ok := statusMsgsInterface.([]Message); ok {
			for _, statusMsg := range statusMsgs {
				buildStatus := NewActionStatusType().NewStatusMessage()
				goalidMsg := statusMsg.(*DynamicMessage).Data()["goal_id"].(*DynamicMessage)
				goalID := NewActionGoalIDType().NewGoalIDMessage().(*DynamicActionGoalID)
				goalID.SetID(goalidMsg.Data()["id"].(string))
				goalID.SetStamp(goalidMsg.Data()["stamp"].(Time))
				buildStatus.SetGoalID(goalID)
				buildStatus.SetStatus(statusMsg.(*DynamicMessage).Data()["status"].(uint8))
				buildStatus.SetStatusText(statusMsg.(*DynamicMessage).Data()["text"].(string))
				statusList = append(statusList, buildStatus)
			}
		}
	}
	status.SetStatusArray(statusList)
	if statusHeaderInterface, ok := statusArray.Data()["header"]; ok {
		if statusHeader, ok := statusHeaderInterface.(Message); ok {
			status.SetHeader(statusHeader)
		}
	}
	return status
}

// Instnatiators for main Action message types. These functions copy dynamic messages into the
// dynamic action message type.
// Create a new Goal message from DynamicActionGoalType
func (a *DynamicActionGoalType) NewGoalMessage() ActionGoal {
	m := DynamicActionGoal{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// NewGoalMessageFromInterface creates an ActionGoal provided an interface.
// Used where serialization/deserialization removes traces of action interface types from a message
func (a *DynamicActionGoalType) NewGoalMessageFromInterface(goal interface{}) ActionGoal {
	goalmsg := goal.(*DynamicMessage)
	actionGoal := a.NewGoalMessage().(*DynamicActionGoal)
	goalid := goalmsg.Data()["goal_id"].(*DynamicMessage)
	goalID := NewActionGoalIDType().NewGoalIDMessage()
	goalID.SetStamp(goalid.Data()["stamp"].(Time))
	goalID.SetID(goalid.Data()["id"].(string))

	actionGoal.SetGoal(goalmsg.Data()["goal"].(Message))
	actionGoal.SetGoalId(goalID)
	actionGoal.SetHeader(goalmsg.Data()["header"].(Message))
	return actionGoal
}

// NewGoalIDMessageFromInterface create an ActionGoalID provided an interface
// Used where serialization/deserialization removes traces of action interface types from a message
func (a *DynamicActionGoalIDType) NewGoalIDMessageFromInterface(goalID interface{}) ActionGoalID {
	goalIDmsg := goalID.(*DynamicMessage)
	actionGoalID := a.NewGoalIDMessage().(*DynamicActionGoalID)

	actionGoalID.SetStamp(goalIDmsg.Data()["stamp"].(Time))
	actionGoalID.SetID(goalIDmsg.Data()["id"].(string))
	return actionGoalID
}

// Create a new Feedback message from DynamicActionFeedbackType
func (a *DynamicActionFeedbackType) NewFeedbackMessage() ActionFeedback {
	m := DynamicActionFeedback{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// NewFeedbackMessageFromInterface creates an ActionFeedback provided an interface.
// Used where serialization/deserialization removes traces of action interface types from a message
func (a *DynamicActionFeedbackType) NewFeedbackMessageFromInterface(feedback interface{}) ActionFeedback {
	feedMsg := feedback.(*DynamicMessage)
	feed := a.NewFeedbackMessage().(*DynamicActionFeedback)
	feed.SetFeedback(feedMsg.Data()["feedback"].(Message))
	feed.SetHeader(feedMsg.Data()["header"].(Message))
	status := NewActionStatusType().NewStatusMessage().(*DynamicActionStatus)
	statusMsg := feedMsg.Data()["status"].(*DynamicMessage)
	goalidMsg := statusMsg.Data()["goal_id"].(*DynamicMessage)
	goalID := NewActionGoalIDType().NewGoalIDMessage().(*DynamicActionGoalID)
	goalID.SetID(goalidMsg.Data()["id"].(string))
	goalID.SetStamp(goalidMsg.Data()["stamp"].(Time))

	status.SetGoalID(goalID)
	status.SetStatus(statusMsg.Data()["status"].(uint8))
	status.SetStatusText(statusMsg.Data()["text"].(string))

	feed.SetStatus(status)
	return feed
}

// Create a new Result message from DynamicActionResultType
func (a *DynamicActionResultType) NewResultMessage() ActionResult {
	m := DynamicActionResult{}
	m.DynamicMessage = *a.DynamicMessageType.NewDynamicMessage()
	return &m
}

// NewResultMessageFromInterface creates an ActionResult provided an interface.
// Used where serialization/deserialization removes traces of action interface types from a message
func (a *DynamicActionResultType) NewResultMessageFromInterface(result interface{}) ActionResult {
	res := a.NewResultMessage().(*DynamicActionResult)
	resultMsg := result.(*DynamicMessage)
	res.SetHeader(resultMsg.Data()["header"].(Message))
	res.SetResult(resultMsg.Data()["result"].(Message))
	status := NewActionStatusType().NewStatusMessage().(*DynamicActionStatus)
	statusMsg := resultMsg.Data()["status"].(*DynamicMessage)
	goalidMsg := statusMsg.Data()["goal_id"].(*DynamicMessage)
	goalID := NewActionGoalIDType().NewGoalIDMessage().(*DynamicActionGoalID)
	goalID.SetID(goalidMsg.Data()["id"].(string))
	goalID.SetStamp(goalidMsg.Data()["stamp"].(Time))

	status.SetGoalID(goalID)
	status.SetStatus(statusMsg.Data()["status"].(uint8))
	status.SetStatusText(statusMsg.Data()["text"].(string))
	res.SetStatus(status)
	return res
}

// Create a new standard header message. Convenience function
func NewActionHeader() Message {
	headerType, _ := NewDynamicMessageType("std_msgs/Header")
	header := headerType.NewMessage().(*DynamicMessage)
	header.Data()["stamp"] = Now()
	return header
}

//	DynamicActionType

// Name returns the full ROS name of the action type; required for actionlib.ActionType.
func (t *DynamicActionType) Name() string { return t.name }

// MD5Sum returns the ROS compatible MD5 sum of the action type; required for actionlib.ActionType.
func (t *DynamicActionType) MD5Sum() string { return t.md5sum }

// Text returns the full ROS text of the action type; required for actionlib.ActionType.
func (t *DynamicActionType) Text() string { return t.text }

// GoalType returns the full ROS MessageType of the action goalType; required for actionlib.ActionType.
func (t *DynamicActionType) GoalType() ActionGoalType { return t.goalType }

// FeedbackType returns the full ROS MessageType of the action feedbackType; required for actionlib.ActionType.
func (t *DynamicActionType) FeedbackType() ActionFeedbackType { return t.feedbackType }

// ResultType returns the full ROS MessageType of the action resultType; required for actionlib.ActionType.
func (t *DynamicActionType) ResultType() ActionResultType { return t.resultType }

// NewAction creates a new DynamicAction instantiating the action type; required for actionlib.ActionType.
func (t *DynamicActionType) NewAction() Action {
	// Don't instantiate actions for incomplete types.
	if t == nil {
		return nil
	}
	// But otherwise, make a new one.
	a := new(DynamicAction)
	a.dynamicType = t
	a.Goal = t.GoalType().NewMessage().(*DynamicActionGoal)
	a.Feedback = t.FeedbackType().NewMessage().(*DynamicActionFeedback)
	a.Result = t.ResultType().NewMessage().(*DynamicActionResult)
	return a
}

//

// GetActionGoal returns the actionlib.ActionGoal of the DynamicAction; required for actionlib.Action.
func (a *DynamicAction) GetActionGoal() ActionGoal { return a.Goal }

// GetActionFeedback returns the actionlib.ActionFeedback of the DynamicAction; required for actionlib.Action.
func (a *DynamicAction) GetActionFeedback() ActionFeedback { return a.Feedback }

// GetActionResult returns the actionlib.ActionResult of the DynamicAction; required for actionlib.Action.
func (a *DynamicAction) GetActionResult() ActionResult { return a.Result }

// Dynamic Action Goal Interface
// Get and set functions of DynamicActionGoal interface
func (m *DynamicActionGoal) SetGoal(goal Message) { m.Data()["goal"] = goal }

func (m *DynamicActionGoal) GetGoal() (Message, error) {
	if val, ok := m.Data()["goal"]; !ok {
		return nil, errors.New("no element 'goal' in data map")
	} else if dm, ok := val.(*DynamicMessage); !ok {
		return nil, errors.New("unable to convert to *DynamicMessage")
	} else {
		return dm, nil
	}
}

func (m *DynamicActionGoal) GetGoalId() (ActionGoalID, error) {
	if val, ok := m.Data()["goal_id"]; !ok {
		return nil, errors.New("no element 'goal_id' in data map")
	} else if id, ok := val.(*DynamicActionGoalID); !ok {
		return nil, errors.New("unable to convert to *DynamicActionGoalID")
	} else {
		return id, nil
	}
}
func (m *DynamicActionGoal) SetGoalId(goalid ActionGoalID) { m.Data()["goal_id"] = goalid }

func (m *DynamicActionGoal) GetHeader() (Message, error) {
	if val, ok := m.Data()["header"]; !ok {
		return nil, errors.New("no element 'header' in data map")
	} else if id, ok := val.(*DynamicMessage); !ok {
		return nil, errors.New("unable to convert to *DynamicMessage")
	} else {
		return id, nil
	}
}

func (m *DynamicActionGoal) SetHeader(header Message) { m.Data()["header"] = header }

// Get and Set Functions of DynamicActionGoalID
func (m *DynamicActionGoalID) GetID() string       { return m.Data()["id"].(string) }
func (m *DynamicActionGoalID) SetID(id string)     { m.Data()["id"] = id }
func (m *DynamicActionGoalID) GetStamp() Time      { return m.Data()["stamp"].(Time) }
func (m *DynamicActionGoalID) SetStamp(stamp Time) { m.Data()["stamp"] = stamp }

//

// Dynamic Action Feedback Interface
// Get and set functions of DynamicActionFeedback interface
func (m *DynamicActionFeedback) SetFeedback(goal Message) { m.Data()["feedback"] = goal }
func (m *DynamicActionFeedback) GetFeedback() Message {
	return m.Data()["feedback"].(*DynamicMessage)
}
func (m *DynamicActionFeedback) GetStatus() ActionStatus {
	return m.Data()["status"].(*DynamicActionStatus)
}
func (m *DynamicActionFeedback) SetStatus(status ActionStatus) { m.Data()["status"] = status }
func (m *DynamicActionFeedback) GetHeader() Message {
	return m.Data()["header"].(*DynamicMessage)
}
func (m *DynamicActionFeedback) SetHeader(header Message) { m.Data()["header"] = header }

//

// Dynamic Action Feedback Interface
// Get and set functions of DynamicActionFeedback interface
func (m *DynamicActionResult) SetResult(result Message) { m.Data()["result"] = result }
func (m *DynamicActionResult) GetResult() Message       { return m.Data()["result"].(*DynamicMessage) }
func (m *DynamicActionResult) GetStatus() ActionStatus {
	return m.Data()["status"].(*DynamicActionStatus)
}
func (m *DynamicActionResult) SetStatus(status ActionStatus) { m.Data()["status"] = status }
func (m *DynamicActionResult) GetHeader() Message {
	return m.Data()["header"].(*DynamicMessage)
}
func (m *DynamicActionResult) SetHeader(header Message) { m.Data()["header"] = header }

// Get and Set Functions of shared type DynamicActionStatus
func (m *DynamicActionStatus) GetGoalID() ActionGoalID {
	return m.Data()["goal_id"].(*DynamicActionGoalID)
}
func (m *DynamicActionStatus) SetGoalID(id ActionGoalID) { m.Data()["goal_id"] = id }
func (m *DynamicActionStatus) GetStatus() uint8          { return m.Data()["status"].(uint8) }
func (m *DynamicActionStatus) SetStatus(status uint8)    { m.Data()["status"] = status }
func (m *DynamicActionStatus) GetStatusText() string     { return m.Data()["text"].(string) }
func (m *DynamicActionStatus) SetStatusText(text string) { m.Data()["text"] = text }

// Get and set functions of the shared type DynamicActionStatusArray
func (m *DynamicActionStatusArray) GetStatusArray() []ActionStatus {
	return m.Data()["status_list"].([]ActionStatus)
}
func (m *DynamicActionStatusArray) SetStatusArray(statusArray []ActionStatus) {
	m.Data()["status_list"] = statusArray
}
func (m *DynamicActionStatusArray) GetHeader() Message {
	return m.Data()["header"].(*DynamicMessage)
}
func (m *DynamicActionStatusArray) SetHeader(header Message) { m.Data()["header"] = header }
