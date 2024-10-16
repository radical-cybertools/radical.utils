#!/bin/sh

# test the shell pwatcher

start=$(date +%s)

sleep 1 &
export RU_PW_PID1=$!

sleep 3 &
export RU_PW_PID2=$!

./bin/radical-utils-pwatch

stop=$(date +%s)
test "$(($stop-$start))" -gt 2 && exit 1 || exit 0

