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

ENGINE_NAME=$1

# first download jar form oss and scp to different node.
echo "download jar from oss"
yes | ossutil cp $OSS_JAR_DIR/$FLUSS_SERVER_JAR_NAME /root
yes | ossutil cp $OSS_JAR_DIR/$OMB_FRAMWORK_JAR_NAME /root

if [ ! -f "$ARTHAS_INSTALL_PATH" ];then
	yes | ossutil cp $OSS_JAR_DIR/$ARTHAS_JAR_NAME /root/tmpJar
else
	echo "arthas exists, will not download from oss"
fi

echo "jar hava downloaded from oss, scp to different node"
for SERVER in "${SERVER_ADDRESSES[@]}"; do
	yes | scp /root/$FLUSS_SERVER_JAR_NAME $SERVER:$FLUSS_INSTALL_PATH/lib/
	yes | scp /root/tmpJar/$ARTHAS_JAR_NAME $SERVER:$ARTHAS_INSTALL_PATH
done

for CLIENT in "${OMB_CLIENT_ADDRESSES[@]}"; do
	yes | scp /root/$OMB_FRAMWORK_JAR_NAME $CLIENT:$OMB_INSTALL_PATH/lib/
	yes | scp /root/tmpJar/$ARTHAS_JAR_NAME $SERVER:$ARTHAS_INSTALL_PATH
done

scp_conf_to_servers() {
    local server=$1
    last_digit=$(echo "$input" | awk '{print substr(\$0,length(\$0),1)}')
    server_id=$((last_digit - 1))
    echo "scp conf to tablet servers on $server with server id $server_id..."

    # copy server.yaml to remote servers
    yes | scp config/server.yaml $server:$FLUSS_INSTALL_PATH/conf/server.yaml

    ssh "$server" << EOF
        cd $FLUSS_INSTALL_PATH
        # modify some values in sever yaml
        sed -i "s|tablet-server.id: 0|tablet-server.id: $server_index\$server_id|" ./conf/server.yaml
        sed -i "s|tablet-server.host: fluss-new-server1|tablet-server.host: $server|" ./conf/server.yaml
EOF
    echo "$server tablet servers started on $server."
}

# scp fluss config to all server.
if [ "$ENGINE_NAME" = "fluss" ]; then
  echo scp fluss config to all server
  for SERVER in "${SERVER_ADDRESSES[@]}"; do
    scp_conf_to_servers $SERVER
  done
fi

echo "begin to start $ENGINE_NAME server"
case "$ENGINE_NAME" in
	fluss)
		echo start fluss ...
		# start coordinator in this server
		$FLUSS_INSTALL_PATH/bin/coordinator-server.sh start
		echo "start fluss coordinator server in this node"
		for SERVER in "${SERVER_ADDRESSES[@]}"; do
			COMMAND="export JAVA_HOME=$JAVA_HOME && $FLUSS_INSTALL_PATH/bin/tablet-server.sh start"
			ssh "$SERVER" "$COMMAND"
			echo "start tablet server in ${SERVER}"
		done
		echo
		;;
	kafka)
		echo start kafka ...
		for SERVER in "${SERVER_ADDRESSES[@]}"; do
			COMMAND="export JAVA_HOME=$JAVA_HOME && $KAFKA_INSTALL_PATH/bin/kafka-server-start.sh -daemon $KAFKA_INSTALL_PATH/config/server.properties"
                        ssh "$SERVER" "$COMMAND"
                        echo "start kafka broker in ${SERVER}"
                done
		echo
		;;
  hologres)
    echo start hologres ..
    ;;
  datahub)
    echo start datahub ..
    ;;
	*)
		echo "Unsupported ENGINE_NAME: \$1"
		echo "ENGINE_NAME can be 'fluss', 'kafka', 'hologres' and 'datahub'"
		exit 1
		;;
	esac

echo "begin to start OMB worker"
for CLIENT in "${OMB_CLIENT_ADDRESSES[@]}"; do
	COMMAND="export JAVA_HOME=$OMB_CLIENT_JAVA_HOME && export PATH=\$JAVA_HOME/bin:\$PATH  && cd $OMB_INSTALL_PATH && nohup ./bin/benchmark-worker --port 10080 --stats-port 10081 > benchmark_worker_output.log 2>&1 &"
	ssh -n -f "$CLIENT" "$COMMAND"
	echo "start OMB worker in ${CLIENT}"
done
