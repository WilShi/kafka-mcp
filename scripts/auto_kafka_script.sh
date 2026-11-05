#!/bin/bash

print_help() {
  echo "Usage: $0 [OPTIONS] COMMAND [ARGS]"
  echo
  echo "Options:"
  echo "  --help     Show this message"
  echo
  echo "Commands:"
  echo "  create-topic          Create a new Kafka topic"
  echo "  list-topics           List all Kafka topics"
  echo "  describe-topic        Describe details of a Kafka topic"

  echo "  delete-topic          Delete a Kafka topic"
  echo "  add-partition         Add partition number to a Kafka topic"
  echo "  create-topic-by-partition  Create a new Kafka topic with partition number"
  echo "  add-topic-config      Add topic config to a Kafka topic"
  echo "  check-topic-config     Check topic config of a Kafka topic"
  echo "  delete-topic-config   Delete topic config from a Kafka topic"
  echo "  start-server           Start a Kafka server"
  echo "  stop-server           Stop a Kafka server"
  echo "  restart-server        Restart a Kafka server"
  echo "  install-auto-pull-up-tool    Install auto pull up tool"



  echo "  cluster-status        Show status of the Kafka cluster"
  echo "  partition-migration   Migrate Kafka topic partitions"
  echo "  view-backlog          View message backlog for a topic"
  echo "  set-acl               Set ACL permissions for a user"
  echo "  create-user           Create a new Kafka user"
  echo "  cluster-benchmark     Run a benchmark on the Kafka cluster"
  echo "  force-leader          Force leader election for a partition"
  echo
  echo "Run '$0 COMMAND --help' for more information on a command."
}




function echo_currect() {
    MISSAGE=$1
    echo -e "\033[32m$MISSAGE\033[0m"
}

function echo_incurrect() {
    MISSAGE=$1
    echo -e "\033[31m$MISSAGE\033[0m"
}

function echo_back_red() {
    MISSAGE=$1
    echo -e "\033[41m$MISSAGE\033[0m"
}


function get_kafka_install_path() {
    kafka_process=$(ps -ef | grep kafka | grep -v grep | grep -v zookeeper.properties | grep server.properties)
    kafka_command=$(echo "$kafka_process" | awk '{for(i=1;i<=NF;i++)if($i~"-Xloggc:")print $i}')
    # 提取-Xloggc: 后面的日志文件路径
    kafka_install_path=$(echo "$kafka_command" | awk -F'-Xloggc:|/bin' '{if (NF>2) print $2}')
    # 如果找到了日志文件路径，则打印结果并向上追溯两级以得到可能的Kafka安装目录
    if [[ -n "$kafka_install_path" ]]; then
        echo "Detected possible Kafka installation at: $kafka_install_path"
        KAFKA_DIR=$kafka_install_path
    else
        echo "Failed to detect Kafka installation directory."
        read -p "Please provide the Kafka installation directory: " KAFKA_DIR
    fi
}


function get_kafka_user() {
    kafka_process=$(ps -ef | grep kafka | grep -v grep | grep -v zookeeper.properties | grep server.properties)
    # 获取启动kafka进程的用户名
    kafka_user=$(echo "$kafka_process" | awk '{print $1}')
    if [ -z "$kafka_user" ]; then
        echo "Failed to detect Kafka user."
        read -p "Please provide the Kafka user: " KAFKA_USER
    else
        echo "Detected Kafka user: $kafka_user"
        KAFKA_USER=$kafka_user
    fi
}



function get_kafka_pid() {
    kafka_process=$(ps -ef | grep kafka | grep -v grep | grep -v zookeeper.properties | grep server.properties)
    KAFKA_PID=$(echo "$kafka_process" | awk '{print $2}')
}


# function execute_command() {
#   if [[ -z "$kafka_cmd" ]]; then
#     echo "Kafka operation command not provided."
#   else
#     eval $kafka_cmd
#   fi
# }


function execute_command() {
  local cmd="$1"  # 使用函数参数代替全局变量
  if [[ -z "$cmd" ]]; then
    echo "Kafka operation command not provided."
  else
    # 使用双引号包围变量，并对变量进行空格分割，以处理包含空格的命令
    # 使用$(...)代替eval，提高安全性，虽然这里的效果和eval相同，但更安全，且易于debug
    echo -e "Executing command: \033[31m$cmd\033[0m"
    if ! $cmd; then  # 直接执行命令，并检查失败情况
      echo "Command execution failed: $cmd"
      exit 1  # 继续使用exit 1来强调错误
    fi
  fi
}



# function get_zookeeper() {
#   read -p "Use local cluster zookeeper? (yes/no): " zookeeper_local
#   local_cmd=false
#   if [[ "$zookeeper_local" == "yes" ]] || [[ "$zookeeper_local" == "y" ]]; then
#     original_zk_path=$(cat "$KAFKA_DIR/config/server.properties" | grep "zookeeper.connect=" | grep -v "^#")
#     ZOOKEEPER_CONNECT=$(echo "$original_zk_path" | sed 's/^zookeeper.connect=//')
#     local_cmd=true
#   else
#     read -p "Enter Zookeeper address (e.g., localhost:2181): " ZOOKEEPER_CONNECT
#   fi

#   if [[ -z "$ZOOKEEPER_CONNECT" ]]; then
#     echo "Zookeeper address not provided."
#     ZOOKEEPER_CONNECT="xxx:xxx"
#   fi
# }


function get_zookeeper() {
  read -p "Use local cluster zookeeper or manually set zookeeper address? (yes/enter address): " zookeeper_local
  check_kafka_dir
  local_cmd=""
  if [[ "$zookeeper_local" == "yes" ]] || [[ "$zookeeper_local" == "y" ]]; then
    original_zk_path=$(cat "$KAFKA_DIR/config/server.properties" | grep "zookeeper.connect=" | grep -v "^#")
    ZOOKEEPER_CONNECT=$(echo "$original_zk_path" | sed 's/^zookeeper.connect=//')
    local_cmd=1
  else
    # read -p "Enter Zookeeper address (e.g., localhost:2181): " ZOOKEEPER_CONNECT
    ZOOKEEPER_CONNECT="$zookeeper_local"
  fi

  if [[ -z "$ZOOKEEPER_CONNECT" ]]; then
    echo "Zookeeper address not provided."
    ZOOKEEPER_CONNECT="xxx:xxx"
  fi
}

function get_broker() {
  read -p "Use local cluster broker? (yes/no): " broker_local
  local_cmd=""
  if [[ "$broker_local" == "yes" ]] || [[ "$broker_local" == "y" ]]; then
    KAFKA_BROKER=$(/sbin/ifconfig -a | grep inet | grep -v inet6 | grep -vE '127\.' | awk '{print $2}' | cut -d: -f2)
    if [[ -z "$KAFKA_BROKER" ]]; then
      KAFKA_BROKER=$(ip addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}(?=/)' | grep -vE '127\.')
      local_cmd=1
    fi
  
  else
    read -p "Enter Kafka Broker address (e.g., localhost:9092): " KAFKA_BROKER
  fi

  if [[ -z "$KAFKA_BROKER" ]]; then
    echo "Kafka broker address not provided."
    KAFKA_BROKER="xxx"
  fi
}



function is_kafka_alive() {
  local original_kafka_pid=$KAFKA_PID
  # 持续检查Kafka进程是否运行
  while true; do
      # 使用jps -l命令查找包含指定主类名的进程
      KAFKA_PID=$(jps -l | grep "kafka.Kafka" | awk '{print $1}')
      if [ -z "$KAFKA_PID" ]; then
          get_kafka_pid
      fi

      if [[ "$original_kafka_pid" != "$KAFKA_PID" && -n "$KAFKA_PID" ]]; then
          echo "Kafka process has been restarted."
          break
      fi

      if [[ -z "$KAFKA_PID" ]]; then
          echo "Kafka process is not running."
          echo "Kafka stopped."
          break  # 当Kafka进程不存在时，使用break命令跳出循环
      else
          echo "Kafka process is running with PID: $KAFKA_PID"
      fi

      sleep 2
  done

  
}

function stop_kafka() {
  check_kafka_dir
  check_kafka_user
  get_kafka_pid

  if [ -z "$KAFKA_PID" ]; then
      get_kafka_pid
  fi

  if [ -z "$KAFKA_PID" ]; then
      echo "Kafka process not running."
  else

      echo "Kafka PID: $KAFKA_PID"
      echo "Stopping Kafka..."
      # 停止kafka
      CURRENT_USER=$(whoami)

      if [ "$CURRENT_USER" != "$KAFKA_USER" ]; then
          sudo -u $KAFKA_USER sh $KAFKA_DIR/bin/kafka-server-stop.sh
      else
          sh $KAFKA_DIR/bin/kafka-server-stop.sh
      fi

      command_status=$(echo $?)
      if [ $command_status -eq 0 ]; then
          is_kafka_alive

      else
          KAFKA_PID=$(jps -l | grep "kafka.Kafka" | awk '{print $1}')
          if [ -z "$KAFKA_PID" ]; then
              get_kafka_pid
          fi
          echo "Failed to stop Kafka."
          echo "Try to use sudo -u $KAFKA_USER kill -15 $KAFKA_PID to kill the process."
          sudo -u $KAFKA_USER kill -15 $KAFKA_PID
          is_kafka_alive
          

      fi
  fi

  local_cmd=2

}



function is_kafka_started() {
    # 移除固定的sleep，改为循环检查，避免不必要的等待
    local timeout=10 # 设置最大等待时间为10秒
    local interval=1 # 检查间隔为1秒
    while [[ $timeout -gt 0 ]]; do
        get_kafka_pid
        if [[ -n "$KAFKA_PID" ]]; then
            echo_back_red "Kafka process is running with PID: $KAFKA_PID"
            sleep 2
            ps -ef | grep kafka | grep -v grep | grep -v zookeeper.properties | grep server.properties
            break
        fi
        sleep $interval
        timeout=$((timeout - interval))
    done

    if [[ -z "$KAFKA_PID" ]]; then
        # 确保使用正确的命令输出错误信息
        echo_incorrect "Kafka process is not running. Please find the problem and manually start it."
    fi
}

function start_kafka {
  CURRENT_USER=$(whoami)
  check_kafka_dir
  check_kafka_user

  if [[ $CURRENT_USER == $KAFKA_USER ]]; then
      start_kafka_as_user
  elif [[ $CURRENT_USER == "root" ]]; then
      start_kafka_as_root
  else
      echo_back_red "Kafka process can only be started by $KAFKA_USER or root user."
      exit 1
  fi

  local_cmd=2
}

function start_kafka_as_user {
    echo "Current is $CURRENT_USER, Start Kafka as $KAFKA_USER..."

    CHECK_KFK_COMMAND=$(crontab -l | grep -v "^#" | grep "check_kfk.sh")
    command=$(echo "$CHECK_KFK_COMMAND" | cut -d ' ' -f7-)

    if [[ -n "$CHECK_KFK_COMMAND" ]]; then
        echo_incurrect "Start Kafka by: $command"
        # 使用数组和"$@"来避免eval
        eval "$command"
        is_kafka_started
    else
        start_kafka_without_crontab
    fi
}

function start_kafka_as_root {
    echo "Current is $CURRENT_USER, Start Kafka as $KAFKA_USER..."

    CHECK_KFK_COMMAND=$(sudo -u $KAFKA_USER crontab -l | grep -v "^#" | grep "check_kfk.sh")
    command=$(echo "$CHECK_KFK_COMMAND" | cut -d ' ' -f7-)

    if [[ -n "$CHECK_KFK_COMMAND" ]]; then
        echo_incurrect "Start Kafka by: sudo -u $KAFKA_USER $command"
        # 使用数组和"$@"来避免eval
        sudo -u $KAFKA_USER "$command"
        is_kafka_started
    else
        start_kafka_without_crontab
    fi
}

function start_kafka_without_crontab {
    echo_incurrect "This node is not properly configured with an autopullup script"
    KAFKA_PID=$(jps -l | grep "kafka.Kafka" | awk '{print $1}')
    if [ -z "$KAFKA_PID" ]; then
        get_kafka_pid
    fi

    if [[ -z "$KAFKA_PID" ]]; then
        echo "Start Kafka by: sh $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties"
        sh $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
        is_kafka_started
    fi
}



function restart_kafka() {
  echo "Restarting Kafka..."

  check_kafka_user
  stop_kafka
  start_kafka

  local_cmd=2

}





create_topic() {
  echo "Generating Kafka command for creating a new topic"
  get_zookeeper
  echo "Creating a new Kafka topic"
  read -p "Enter topic name: " topic_name
  read -p "Enter number of partitions: " num_partitions
  read -p "Enter replication factor: " replication_factor

  kafka_cmd="kafka-topics.sh --create --topic $topic_name --partitions $num_partitions --replication-factor $replication_factor --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

list_topics() {
  echo "Generating Kafka command for listing all topics"
  get_zookeeper
  echo "Listing all Kafka topics"
  kafka_cmd="kafka-topics.sh --list --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

describe_topic() {
  echo "Generating Kafka command for describing a topic"
  get_zookeeper
  echo "Describing a Kafka topic"
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-topics.sh --describe --topic $topic_name --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

delete_topic() {
  echo "Generating Kafka command for deleting a topic"
  get_zookeeper
  echo "Deleting a Kafka topic"
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-topics.sh --delete --topic $topic_name --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

add_partition() {
  echo "Generating Kafka command for adding partitions to a topic"
  get_zookeeper
  echo "Adding a partition to a Kafka topic"
  read -p "Enter topic name: " topic_name
  read -p "Enter partition number: " partition_number
  kafka_cmd="kafka-topics.sh --alter --topic $topic_name --partitions $partition_number --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

create_topic_by_partition() {
  echo "Generating Kafka command for creating a Kafka topic with a specific number of partitions"
  get_zookeeper
  echo "Creating a Kafka topic with a specific number of partitions"
  read -p "Enter topic name: " topic_name
  read -p "Enter replica assignment(1:2,3:4): " replica_assignment
  kafka_cmd="kafka-topics.sh --create --topic $topic_name --replica-assignment $replica_assignment --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

function choice_topic_config() {
  # Provide a list of Kafka Topic configurations along with their indices
  configs=("min.insync.replicas" 
          "retention.ms" 
          "retention.bytes" 
          "segment.ms" 
          "segment.bytes" 
          "cleanup.policy" 
          "flush.messages" 
          "flush.ms" 
          "unclean.leader.election.enable" 
          "max.message.bytes" 
          "message.format.version" 
          "compression.type" 
          "delete.retention.ms" 
          "file.delete.delay.ms" 
          "index.interval.bytes" 
          "leader.replication.throttled.replicas")

  # Display the options and prompt the user to choose
  echo ""
  echo "Please select a Kafka Topic configuration:"
  for ((i=1; i<=${#configs[@]}; i++)); do
      echo "$i. ${configs[i-1]}"
  done
  echo ""

  read -p "Enter the option number or configuration name: " choice

  # Check if the user's input is a valid number or configuration name
  valid=false
  if [[ "$choice" =~ ^[0-9]+$ ]]; then
      # If the input is a number, subtract 1 to match the array index
      selected_index=$((choice - 1))
      if ((selected_index >= 0 && selected_index < ${#configs[@]})); then
          selected_config=${configs[selected_index]}
          valid=true
      fi
  else
      for config in "${configs[@]}"; do
          if [[ "$choice" == "$config" ]]; then
              selected_config=$config
              valid=true
              break
          fi
      done
  fi

  if [ "$valid" = true ]; then
      # echo "$selected_config"
      echo "You have chosen the configuration: $selected_config"
  else
      echo "Invalid selection"
      exit 1
  fi
}

add_topic_config() {
  echo "Generating Kafka command for adding a config to a topic"
  get_zookeeper
  echo "Adding a config to a Kafka topic"
  read -p "Enter topic name: " topic_name
  choice_topic_config
  # topic_config="$selected_config"
  read -p "Enter config value: " config_value
  kafka_cmd="kafka-configs.sh --alter --entity-type topics --entity-name $topic_name --add-config \"$selected_config=$config_value\" --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"

}

check_topic_config() {
  echo "Generating Kafka command for checking a config on a topic"
  get_zookeeper
  echo "Checking a config on a Kafka topic"
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-configs.sh --describe --entity-type topics --entity-name $topic_name --zookeeper $ZOOKEEPER_CONNECT"
  echo "Generated Kafka command: $kafka_cmd"
}

delete_topic_config() {
  echo "Generating Kafka command for deleting a config from a topic"
  get_zookeeper
  echo "Deleting a config from a Kafka topic"
  read -p "Enter topic name: " topic_name
  choice_topic_config
  # topic_config="$selected_config"
  kafka_cmd="kafka-configs.sh --alter --entity-type topics --entity-name $topic_name --delete-config \"$selected_config\" --zookeeper $ZOOKEEPER_CONNECT"
}


# start_server() {
#   echo "Generating Kafka command for starting the Kafka server"
#   get_broker
#   echo "Starting Kafka server"
#   kafka_cmd="sudo systemctl start kafka"
#   echo "Generated Kafka command: $kafka_cmd"
# }




cluster_status() {
  echo "Generating Kafka command for showing the Kafka cluster status"
  get_broker
  echo "Showing Kafka cluster status"
  check_kafka_dir
  # kafka_cmd="kafka-topics.sh --describe --zookeeper $ZOOKEEPER_CONNECT"

  brokers_addresses=$(echo "$KAFKA_BROKER" | tr ',' ' ')
  for server in $brokers_addresses; do
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $server --list > /dev/null && echo "$server is online" || echo "$server is offline"
  done

  # echo "Generated Kafka command: $kafka_cmd"
}



partition_migration() {
  echo "Generating Kafka command for migrating Kafka topic partitions"
  get_broker
  echo "Migrating Kafka topic partitions"
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-reassign-partitions.sh --topics $topic_name --broker-list $KAFKA_BROKER --generate"
  echo "Generated Kafka command: $kafka_cmd"
}

view_backlog() {
  echo "Generating Kafka command for viewing message backlog for a topic"
  get_broker
  echo "Viewing message backlog for a topic"
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-log-dirs.sh --describe --topic $topic_name --bootstrap-server $KAFKA_BROKER"
  echo "Generated Kafka command: $kafka_cmd"
}

set_acl() {
  echo "Generating Kafka command for setting ACL permissions for a user"
  get_zookeeper
  echo "Setting ACL permissions for a user"
  read -p "Enter principal (user): " principal
  read -p "Enter operation (Read/Write/Describe/Trans): " operation
  read -p "Enter topic name: " topic_name

  if [[ "$operation" == "Read" || "$operation" == "read" ]]; then
    kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:$principal --operation $operation --topic $topic_name --group '*'"
  elif [[ "$operation" =~ ^(Write|write|Describe|describe)$ ]]; then
    kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:$principal --operation $operation --topic $topic_name"
  elif [[ "$operation" =~ ^(Trans|trans)$ ]]; then
    kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:$principal --operation $operation --topic $topic_name --transactional-id '*'"
  else
    echo "Invalid operation"
    exit 1
  fi
  echo "Generated Kafka command: $kafka_cmd"
}

set_producer() {
  echo "Generating Kafka command for setting producer permissions"
  get_zookeeper
  echo "Setting producer permissions for a user"
  read -p "Enter principal (user): " principal
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:$principal --producer  --topic $topic_name"
  echo "Generated Kafka command: $kafka_cmd"
}

set_consumer() {
  echo "Generating Kafka command for setting consumer permissions"
  get_zookeeper
  echo "Setting consumer permissions for a user"
  read -p "Enter principal (user): " principal
  read -p "Enter topic name: " topic_name
  kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:$principal --consumer --topic $topic_name --group '*'"
  
}

set_anonymous_permissions_producer() {
  echo "Generating Kafka command for setting anonymous permissions"
  get_zookeeper
  echo "Setting permissions for anonymous user"
  kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:ANONYMOUS --producer --topic '*'"
}

set_anonymous_permissions_consumer() {
  echo "Generating Kafka command for setting anonymous permissions"
  get_zookeeper
  echo "Setting permissions for anonymous user"
  kafka_cmd="kafka-acls.sh --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --add --allow-principal User:ANONYMOUS --consumer --topic '*' --group '*'"
}



create_user() {
  echo "Generating Kafka command for creating a new user"
  get_zookeeper
  echo "Creating a new Kafka user"
  read -p "Enter username: " username
  read -p "Enter password: " password

  kafka_cmd="kafka-configs.sh --zookeeper $ZOOKEEPER_CONNECT --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=$password],SCRAM-SHA-512=[password=$password]' --entity-type users --entity-name $username"
  echo "Generated Kafka command: $kafka_cmd"
}

cluster_benchmark() {
  echo "Generating Kafka command for running a benchmark on the Kafka cluster"
  get_broker
  echo "Running a benchmark on the Kafka cluster"
  read -p "Enter topic name: " topic_name

  kafka_cmd="kafka-producer-perf-test.sh --topic $topic_name --num-records 1000000 --record-size 1000 --throughput -1 --producer-props bootstrap.servers=$KAFKA_BROKER"
  echo "Generated Kafka command: $kafka_cmd"
}

force_leader() {
  echo "Generating Kafka command for forcing leader election for a partition"
  get_zookeeper
  echo "Forcing leader election for a partition"
  read -p "Enter topic name: " topic_name
  read -p "Enter partition number: " partition_number

  if [ -z "$topic_name" ]; then
    kafka_cmd="kafka-preferred-replica-election.sh --zookeeper $ZOOKEEPER_CONNECT"
  else
    kafka_cmd="kafka-preferred-replica-election.sh --zookeeper $ZOOKEEPER_CONNECT --path-to-json-file <(echo '{\"partitions\":[{\"topic\":\"$topic_name\",\"partition\":$partition_number}]}')"
  fi

  echo "Generated Kafka command: $kafka_cmd"
}



function show_options() {
  commands=(
    "1 create-topic"
    "2 list-topics"
    "3 describe-topic"
    "4 delete-topic"
    "5 add-partition"
    "6 create-topic-by-partition"
    "7 add-topic-config"
    "8 check-topic-config"
    "9 delete-topic-config"
    "10 install-auto-pull-up-tool"
    "11 cluster-status"
    "12 partition-migration"
    "13 view-backlog"
    "14 set-acl"
    "15 create-user"
    "16 cluster-benchmark"
    "17 force-leader"
    "18 start-server"
    "19 stop-server"
    "20 restart-server"
    )

  echo ""
  echo_back_red "Available commands:"
  for cmd in "${commands[@]}"; do
      echo "$cmd"
  done

  echo ""

}


function get_local_env() {
  get_kafka_install_path
  get_kafka_user
  get_kafka_pid
}

function check_kafka_dir() {
  if [[ -z "$KAFKA_DIR" ]]; then
    get_kafka_install_path
  fi
  if [[ ! -d "$KAFKA_DIR" ]]; then
    echo "Error: KAFKA_DIR is not a valid directory."
    exit 1
  fi
}

function check_kafka_user() {
  if [ -z "$KAFKA_USER" ]; then
    get_kafka_user
  fi
  if [[ ! -d "/home/$KAFKA_USER" ]]; then
    echo "Error: KAFKA_USER is not a valid user."
    exit 1
  fi
}



main() {

  
  KAFKA_DIR=""
  KAFKA_USER=""

  # get_zookeeper

  case "$1" in
    --help)
      print_help
      exit 0
      ;;
    -help)
      print_help
      exit 0
      ;;
    -h)
      print_help
      exit 0
      ;;
    *)
      if [[ -n "$1" ]]; then
        echo "Invalid command or number. Use '--help' to see available commands."
        exit 1
      fi
      ;;
  esac

  show_options
  read -p "Enter command or number: " choice

  local_cmd=""

  # 根据命令执行操作
  case "$choice" in
    create-topic)
      create_topic
      ;;
    1)
      create_topic
      ;;

    list-topics)
      list_topics
      ;;
    2)
      list_topics
      ;;

    describe-topic)
      describe_topic
      ;;
    3)
      describe_topic
      ;;

    delete-topic)
      delete_topic
      ;;
    4)
      delete_topic
      ;;

    add-partition)
      add_partition
      ;;
    5)
      add_partition
      ;;

    create-topic-by-partition)
      create_topic_by_partition
      ;;
    6)
      create_topic_by_partition
      ;;

    add-topic-config)
      add_topic_config
      ;;
    7)
      add_topic_config
      ;;

    check-topic-config)
      check_topic_config
      ;;
    8)
      check_topic_config
      ;;

    delete-topic-config)
      delete_topic_config
      ;;
    9)
      delete_topic_config
      ;;

    install-auto-pull-up-tool)
      install_auto_pull_up_tool
      ;;
    10)
      install_auto_pull_up_tool
      ;;

    cluster-status)
      cluster_status
      ;;
    11)
      cluster_status
      ;;

    partition-migration)
      partition_migration
      ;;
    12)
      partition_migration
      ;;

    view-backlog)
      view_backlog
      ;;
    13)
      view_backlog
      ;;

    set-acl)
      set_acl
      ;;
    14)
      set_acl
      ;;
    
    create-user)
      create_user
      ;;
    15)
      create_user
      ;;

    cluster-benchmark)
      cluster_benchmark
      ;;
    16)
      cluster_benchmark
      ;;

    force-leader)
      force_leader
      ;;
    17)
      force_leader
      ;;

    start-server)
      start_kafka
      ;;
    18)
      start_kafka
      ;;
    
    stop-server)
      stop_kafka
      ;;
    19)
      stop_kafka
      ;;

    restart-server)
      restart_kafka
      ;;
    20)
      restart_kafka
      ;;

    *)
      echo "Invalid command or number. Use '$0' to see available commands."
      exit 1
      ;;
  esac

  # 函数调用后，检查local_cmd的值
  if [[ "$local_cmd" == "1" ]]; then
    check_kafka_dir

    echo_back_red "Executing command"
    echo ""
    execute_command "$KAFKA_DIR/bin/$kafka_cmd"

  else

    if [[ "$local_cmd" == "2" ]]; then
      exit 0
    fi

    read -p "Execute command? [yes/no] " answer
    if [[ "$answer" == "yes" || "$answer" == 'y' ]]; then
      check_kafka_dir

      echo_back_red "Executing command"
      echo ""
      execute_command "$KAFKA_DIR/bin/$kafka_cmd"
    else
      echo_back_red "Command not executed."
    fi
  fi

}

main "$@"


