#!/bin/bash

if [ "$1" != "" ]; then
  # Launch planning_context in background and wait for it to be ready
  roslaunch "$1" planning_context.launch load_robot_description:=true &
  PLANNING_PID=$!

  # Wait a moment for planning_context to initialize
  sleep 2

  # Launch move_group
  roslaunch "$1" move_group.launch allow_trajectory_execution:=true fake_execution:=true info:=true &
  MOVEGROUP_PID=$!

  # Wait for both processes
  wait $PLANNING_PID $MOVEGROUP_PID

else
  echo "$0: Please provide a moveit package for your robot:"
fi
