#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

HERE="`dirname \"$0\"`"				# relative
HERE="`( cd \"$HERE\" && pwd )`" 	# absolutized and normalized
if [ -z "$HERE" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

# Number of seconds w/o output before printing a stack trace and killing $MVN
MAX_NO_OUTPUT=${1:-300}

# Number of seconds to sleep before checking the output again
SLEEP_TIME=20

MVN_COMPILE="mvn clean install -DskipTests"
MVN_TEST="mvn verify"

ARTIFACTS_DIR="${HERE}/artifacts"
mkdir -p $ARTIFACTS_DIR || { echo "FAILURE: cannot create log directory '${ARTIFACTS_DIR}'." ; exit 1; }

MVN_PID="${ARTIFACTS_DIR}/watchdog.mvn.pid"
MVN_EXIT="${ARTIFACTS_DIR}/watchdog.mvn.exit"
MVN_OUT="${ARTIFACTS_DIR}/mvn.out"

TRACE_OUT="${ARTIFACTS_DIR}/jps-traces.out"

print_stacktraces () {
	echo "=============================================================================="
	echo "The following Java processes are running (JPS)"
	echo "=============================================================================="

	jps

	local pids=( $(jps | awk '{print $1}') )

	for pid in "${pids[@]}"; do
		echo "=============================================================================="
		echo "Printing stack trace of Java process ${pid}"
		echo "=============================================================================="

		jstack $pid
	done
}

mod_time () {
	if [[ `uname` == 'Darwin' ]]; then
		eval $(stat -s $MVN_OUT)
		echo $st_mtime
	else
		echo `stat -c "%Y" $MVN_OUT`
	fi
}

the_time() {
	echo `date +%s`
}

watchdog () {
	touch $MVN_OUT

	while true; do
		sleep $SLEEP_TIME

        echo "the_time"
        the_time
        echo "mod_time"
        mod_time
		time_diff=$((`the_time` - `mod_time`))

		if [ $time_diff -ge $MAX_NO_OUTPUT ]; then

			echo "=============================================================================="
			echo "Maven produced no output for ${MAX_NO_OUTPUT} seconds."
			echo "=============================================================================="

			print_stacktraces | tee $TRACE_OUT

			kill $(<$MVN_PID)

			exit 1
		fi
	done
}

# =============================================================================
# WATCHDOG
# =============================================================================

# Start watching $MVN_OUT
watchdog &

WD_PID=$!

echo "STARTED watchdog (${WD_PID})."


# Compile modules

echo "RUNNING '${MVN_COMPILE}'."

# Run $MVN_COMPILE and pipe output to $MVN_OUT for the watchdog. The PID is written to $MVN_PID to
# allow the watchdog to kill $MVN if it is not producing any output anymore. $MVN_EXIT contains
# the exit code. This is important for Travis' build life-cycle (success/failure).
( $MVN_COMPILE & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$MVN_PID 4>$MVN_EXIT | tee $MVN_OUT

EXIT_CODE=$(<$MVN_EXIT)

echo "MVN exited with EXIT CODE: ${EXIT_CODE}."

rm $MVN_PID
rm $MVN_EXIT

if [ $EXIT_CODE == 0 ]; then

    echo "RUNNING '${MVN_TEST}'."

    # Run $MVN_TEST and pipe output to $MVN_OUT for the watchdog. The PID is written to $MVN_PID to
    # allow the watchdog to kill $MVN if it is not producing any output anymore. $MVN_EXIT contains
    # the exit code. This is important for Travis' build life-cycle (success/failure).
    ( $MVN_TEST & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$MVN_PID 4>$MVN_EXIT | tee $MVN_OUT

    EXIT_CODE=$(<$MVN_EXIT)

    echo "MVN exited with EXIT CODE: ${EXIT_CODE}."

    rm $MVN_PID
    rm $MVN_EXIT
else
    echo "=============================================================================="
    echo "Compilation failure detected, skipping test execution."
    echo "=============================================================================="
fi

# Post

# Make sure to kill the watchdog in any case after $MVN_COMPILE and $MVN_TEST have completed
echo "Trying to KILL watchdog (${WD_PID})."
( kill $WD_PID 2>&1 ) > /dev/null

# Exit code for Travis build success/failure
exit $EXIT_CODE
