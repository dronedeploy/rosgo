#!/bin/bash
source /opt/ros/melodic/setup.bash
export PATH=$PWD/bin:/usr/local/go/bin:$PATH
export GOPATH=$PWD:/usr/local/go

roscore &
go install github.com/dronedeploy/rosgo/gengo
go generate github.com/dronedeploy/rosgo/test/test_message
go test github.com/dronedeploy/rosgo/xmlrpc
go test github.com/dronedeploy/rosgo/ros
go test github.com/dronedeploy/rosgo/test/test_message

