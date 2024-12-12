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

export KAFKA_INSTALL_PATH="/root/kafka-3.7"
export KAFKA_DATA_PATH="/mnt/kafka-logs"
export ARTHAS_INSTALL_PATH="/root/arthas-boot.jar"

export FLUSS_INSTALL_PATH="/root/fluss-log-bench"
export FLUSS_DATA_PATH="/mnt/fluss-log-bench"

export ZK_SERVER_ADDRESS="mse-518cb6e0-zk.mse.aliyuncs.com"
export ZK_NODES_TO_DELETE=("kafka-benchmark-new" "fluss-log-bench-new")

export SERVER_ADDRESSES=("fluss-new-server1" "fluss-new-server2" "fluss-new-server3")
export OMB_CLIENT_ADDRESSES=("fluss-client1" "fluss-client2" "fluss-client3" "fluss-client4")
export OMB_CLIENT_BENCHMARK_EXECUTOR="fluss-client1"
export OMB_INSTALL_PATH="/root/om-benchmark"
export OMB_CLIENT_JAVA_HOME="/root/jdk-11.0.22"

# zkCli related
export ZKCLI_DIR="zk-console"
export ZKCLI_FILE_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz"
export ZKCLI_TAR_FILE="apache-zookeeper-3.7.2-bin.tar.gz"
export ZKCLI_EXTRACTED_DIR="apache-zookeeper-3.7.2-bin"
export ZKCLI_TARGET_DIR="apache-zookeeper"

# jar related
export FLUSS_SERVER_JAR_NAME="fluss-server-0.2-SNAPSHOT.jar"
export OMB_FRAMWORK_JAR_NAME="benchmark-framework-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
export ARTHAS_JAR_NAME="arthas-boot.jar"

# oss related
export OSS_JAR_DIR="oss://yunxie-vvp/fluss-log-benchmark/jar"
export OSS_RESULT_DIR="oss://yunxie-vvp/fluss-log-benchmark/result"
