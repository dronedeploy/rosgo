#!/bin/bash
source /opt/ros/melodic/setup.bash
export PATH=$PWD/bin:/usr/local/go/bin:$PATH
export GOPATH=$PWD:/usr/local/go

roscore &
go install github.com/team-rocos/rosgo/gengo
go generate github.com/team-rocos/rosgo/test/test_message
go test github.com/team-rocos/rosgo/xmlrpc
go test github.com/team-rocos/rosgo/ros
go test github.com/team-rocos/rosgo/test/test_message

