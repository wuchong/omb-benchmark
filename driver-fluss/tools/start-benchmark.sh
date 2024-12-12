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

if [ -z "$1" ]; then
        echo "Usage: $0 <ENGINE_NAME>"
        echo "ENGINE_NAME can be 'fluss' , 'kafka', 'hologres' or 'datahub'"
        exit 1
fi

if [ -z "$2" ]; then
        echo "Usage: $0 <ENGINE_NAME> <CONFIG_YAML>"
        echo "CONFIG_YAML like config/fluss.yaml or config/kafka-throughput.yaml or config/hologres.yaml"
        exit 1
fi

if [ -z "$3" ]; then
        echo "Usage: $0 <ENGINE_NAME> <CONFIG_YAML> <WORKLOAD_YAML>"
        echo "WORKLOAD_YAML like workload/1-topic-100-partitions-1kb-4p-4c-2000k.yaml or workload/max-rate-1-topic-1-partition-4p-1c-1kb.yaml"
        exit 1
fi

ENGINE_NAME=$1
CONFIG_YAML=$2
WORKLOAD_YAML=$3

./clear-env.sh
./start-cluster.sh $ENGINE_NAME

# Start a benchmark jobs
# 1. scp CONFIG_YAML and WORKLOAD_YAML to worker1

CONFIG_YAML_NAME=$(echo "$CONFIG_YAML" | rev | cut -d/ -f1 | rev)
WORKLOAD_YAML_NAME=$(echo "$WORKLOAD_YAML" | rev | cut -d/ -f1 | rev)

ssh "$OMB_CLIENT_BENCHMARK_EXECUTOR" "rm -rf $OMB_INSTALL_PATH/config && mkdir $OMB_INSTALL_PATH/config"
yes | scp $CONFIG_YAML $OMB_CLIENT_BENCHMARK_EXECUTOR:$OMB_INSTALL_PATH/config
yes | scp $WORKLOAD_YAML $OMB_CLIENT_BENCHMARK_EXECUTOR:$OMB_INSTALL_PATH/workloads


# start arthas profiler in all machine.
SERVER_PROCESS_NAME=""
if [ "$ENGINE_NAME" = "fluss" ]; then
    SERVER_PROCESS_NAME="TabletServer"
elif [ "$ENGINE_NAME" = "kafka" ]; then
    SERVER_PROCESS_NAME="Kafka"
else
    echo "$ENGINE_NAME no need to kill processor"
fi

start_profiler() {
	ssh "$1" "export JAVA_HOME=$3 && export PATH=\$JAVA_HOME/bin:\$PATH && nohup sh -c 'java -jar /root/$ARTHAS_JAR_NAME $2 <<< \"profiler start --duration 120\"' &> /dev/null &"
}


for SERVER in "${SERVER_ADDRESSES[@]}"; do
	 COMMAND="ps -ef | grep java | grep \"$SERVER_PROCESS_NAME\" | grep -v grep | awk '{print \$2}'"
	 PID=$(ssh "$SERVER" "$COMMAND")
	 if [ -z "$PID" ]; then
		 echo "No process $SERVER_PROCESS_NAME while begin arthas profiler"
		 exit 1
	 else
		 start_profiler "$SERVER" "$PID" "$JAVA_HOME" &
		 echo "profiler start in $SERVER, last 120 secondes"
	fi
done

for WORKER in "${OMB_CLIENT_ADDRESSES[@]}"; do
	COMMAND="ps -ef | grep java | grep BenchmarkWorker | grep -v grep | awk '{print \$2}'"
	PID=$(ssh "$WORKER" "$COMMAND")
        if [ -z "$PID" ]; then
                 echo "No process BenchmarkWorker while begin arthas profiler"
                 echo "no arthas need to start"
         else
                 start_profiler "$WORKER" "$PID" "$OMB_CLIENT_JAVA_HOME" &
                 echo "profiler start in $WORKER, last 120 secondes"
        fi
done


# start one benchmark job with input config.
COMMAND="export JAVA_HOME=$OMB_CLIENT_JAVA_HOME && export PATH=\$JAVA_HOME/bin:\$PATH  && cd $OMB_INSTALL_PATH && ./bin/benchmark --drivers config/$CONFIG_YAML_NAME --workers http://192.168.10.71:10080,http://192.168.10.72:10080,http://192.168.10.73:10080,http://192.168.10.74:10080 workloads/$WORKLOAD_YAML_NAME"
ssh "$OMB_CLIENT_BENCHMARK_EXECUTOR" "$COMMAND"


# upload the result into oss, the file name begin with date.
# copy running result to result dir.
rm -rf result/*
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [ ! -d "result" ]; then
	mkdir -p result
fi
mkdir -p result/$TIMESTAMP

for SERVER in "${SERVER_ADDRESSES[@]}"; do
	mkdir result/$TIMESTAMP/$SERVER
	scp -r $SERVER:/root/arthas-output/* result/$TIMESTAMP/$SERVER/
	scp -r $SERVER:$FLUSS_INSTALL_PATH/log/* result/$TIMESTAMP/$SERVER/
done

for CLIENT in "${OMB_CLIENT_ADDRESSES[@]}"; do
	mkdir result/$TIMESTAMP/$CLIENT
	scp -r $CLIENT:$OMB_INSTALL_PATH/arthas-output/* result/$TIMESTAMP/$CLIENT/
	scp -r $CLIENT:$OMB_INSTALL_PATH/benchmark_worker_output.log result/$TIMESTAMP/$CLIENT/
done

scp -r $OMB_CLIENT_BENCHMARK_EXECUTOR:$OMB_INSTALL_PATH/*.json result/$TIMESTAMP/$OMB_CLIENT_BENCHMARK_EXECUTOR/

# upload to oss
ossutil cp -r result/$TIMESTAMP oss://yunxie-vvp/fluss-log-benchmark/result/$TIMESTAMP
echo "Benchmark finished, the result and the log is upload to oss://yunxie-vvp/fluss-log-benchmark/result/$TIMESTAMP"
