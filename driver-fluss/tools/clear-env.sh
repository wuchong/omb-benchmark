#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

source ./env.sh

export PROCESS_NAMES=("Kafka" "CoordinatorServer" "TabletServer" "arthas-boot.jar")
export WORKER_PROCESS_NAMES=("Benchmark" "BenchmarkWorker" "arthas-boot.jar")

# download zkCli package
if [ ! -d "$ZKCLI_DIR" ]; then
	echo "zkCli not exist. Download and Creating..."
       	mkdir "$ZKCLI_DIR"
	
	cd "$ZKCLI_DIR"
	echo "Downloading $ZKCLI_FILE_URL..."
	wget "$ZKCLI_FILE_URL"
	if [ $? -ne 0 ]; then
		echo "Failed to download the file from $FILE_URL."
		exit 1
	fi

	# un-zip
	tar -zvxf "$ZKCLI_TAR_FILE"
	mv "$ZKCLI_EXTRACTED_DIR" "$ZKCLI_TARGET_DIR"
	cd ..
else
	echo "zkCli exist. do nothing"
fi
echo

# kill server process and delete data and log
for SERVER in "${SERVER_ADDRESSES[@]}"; do
	# kill server process
	echo "$SERVER:"
	for PROCESS in "${PROCESS_NAMES[@]}"; do
		COMMAND="ps -ef | grep java | grep \"$PROCESS\" | grep -v grep | awk '{print \$2}'"
		PID=$(ssh "$SERVER" "$COMMAND")
		if [ -z "$PID" ]; then
			echo "No process $PROCESS"
		else
            		ssh "$SERVER" "kill -9 $PID"
           		echo "killed process $PROCESS"
            		sleep 1 
		fi
	done

	# delete arthas result
	COMMAND="rm -rf /root/arthas-output"
	ssh "$SERVER" "$COMMAND"

	# delete data.
	COMMAND="rm -rf $KAFKA_DATA_PATH"
	ssh "$SERVER" "$COMMAND"
	echo "delete data for kafka"
	COMMAND="rm -rf $FLUSS_DATA_PATH"
	ssh "$SERVER" "$COMMAND"
	echo "delete data for fluss"

	# delete log.
	COMMAND="rm -rf $FLUSS_INSTALL_PATH/log/*"
	ssh "$SERVER" "$COMMAND"
        echo "delete log for fluss"

	# print empty row
	echo
done

# kill client worker process add delete log
for WORKER in "${OMB_CLIENT_ADDRESSES[@]}"; do
	# kill process
	echo "$WORKER:"
	for PROCESS in "${WORKER_PROCESS_NAMES[@]}"; do
		COMMAND="ps -ef | grep java | grep \"$PROCESS\" | grep -v grep | awk '{print \$2}'"
		PID=$(ssh "$WORKER" "$COMMAND")
		if [ -z "$PID" ]; then
			echo "No process $PROCESS"
		else
			ssh "$WORKER" "kill -9 $PID"
                	echo "killed process $PROCESS"
                	sleep 1
        	fi
	done

	# delete arthas result
	COMMAND="rm -rf $OMB_INSTALL_PATH/arthas-output"
        ssh "$WORKER" "$COMMAND"

	# delete the json file
	COMMAND="rm -rf $OMB_INSTALL_PATH/*.json"
	ssh "$WORKER" "$COMMAND"

	# delete the gz log file
	COMMAND="rm -rf $OMB_INSTALL_PATH/*.gz"
  ssh "$WORKER" "$COMMAND"

  # delete jar in lib
  COMMAND="rm -rf $OMB_INSTALL_PATH/lib/*.jar"
  ssh "$WORKER" "$COMMAND"

	# delete log
	COMMAND="rm -rf $OMB_INSTALL_PATH/benchmark-worker.log $OMB_INSTALL_PATH/benchmark_worker_output.log"
	ssh "$WORKER" "$COMMAND"
        echo "delete log for OMB worker"

	# print empty row
	echo
done

# delete node in zk to avoid data conflict.
delete_node() {
	local base_path=$1

	echo "Deleting zk node /$base_path"
	echo "deleteall /$base_path" | ./$ZKCLI_DIR/$ZKCLI_TARGET_DIR/bin/zkCli.sh -server $ZK_SERVER_ADDRESS
}

for base_path in "${ZK_NODES_TO_DELETE[@]}"; do
	delete_node "$base_path"
done

echo "Environment initialize process completed."
echo

