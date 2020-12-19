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

<<COMMENT
这个方法主要是返回两个变量值,通过echo的方式。
FLINK_CLASSPATH和FLINK_DIST.
这两个变量有什么作用.
COMMENT
constructFlinkClassPath() {
    local FLINK_DIST
    local FLINK_CLASSPATH

<<COMMENT
    错误理解：有个问题哈 jarfile 是哪里定义的?  jarfile不是哪里定义的，是要求是jar类型文件：-r jarfile  要求是jar类型文件
    -d 代表分隔符，这里是按照空格分割
    -r 要求是jar类型文件

    上面的理解是错误的，其实jarfile是一个变量，谁给它赋值呢？是<(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)这个命令给赋值的，
    你也可以在done后面接入一个文件，那么也会读取文件每一行来赋值，比如：
    while read line;do ; echo $line; done < t.txt

    而-d确实是代表分隔符，这里需要注意，比如文件内容是a,b,c 那么-d, 不会吧c打出来，需要在c后面也加一个， 比如： a,b,c,  eg:
    while read -d, line;do ; echo $line; done < <(echo "a,b,c")
    输出：
    a
    b

    while read -d, line;do ; echo $line; done < <(echo "a,b,c,")
    输出：
    a
    b
    c

    那么-r到底是什么含义呢？之前以为是文件格式 ，后面接文件类型，现在看起来是错误的。
    read其实就是一个命令，read line 表示接受终端输入并赋值给line变量,eg:
    ➜  ~ read -r line
    wocao
    ➜  ~ echo $line
    wocao

    而-d和-r都是read命令传入参数，read命令参数有：
    -d 后面跟一个标志符，其实只有其后的第一个字符有用，作为结束的标志。
    作为每次读取结束的标识符号，也就是说读取到哪里结束，然后赋值给变量。只有读取到该结束符后，才会结束读取，赋值给变量，
    否则不会结束并赋值给变量，这也是为啥上面如果c后面没有逗号，就不会打出c的原因：while read -d, line;do ; echo $line; done < <(echo "a,b,c,")
    -r 屏蔽\，如果没有该选项，则\作为一个转义字符，有的话 \就是个正常的字符了。
	参考： http://note.youdao.com/s/Yneyew1P

	接下来是这个命令：<(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)
	如果是从文件读取那么done后面只会有一个< ，但是如果是通过命令获取内容，那么还得加个< ,并且用括号把命令括起来，eg：
	 while read line; do echo $line ; done < <(echo "aa")
	 但为啥要加个<，感觉还没找到个合理的解释。。。

	 <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)这个命令的含义是把jar包全部列出来.

	 find 命令，有一些参数 ,这里 "$FLINK_LIB_DIR" 表示要查找的目录位置，  !是一个否定参数，否定接下来紧挨着的参数,
	 -type d 表示目录，-name '*.jar' 表示以.jar结尾的。
	 -print0：意思就是如果找到了符合条件的文件或者目录，那么按照同一行的格式输出到标准输出中。
	 最后sort -z  表示以NUL作为记录分隔符。 默认情况下，文件中的记录应以换行符分隔。使用此选项时，NUL（'\ 0'）用作记录分隔符。

	 参考：https://man.linuxde.net/find


	 这个while循环实际就是在构建不同类型的classpath,
	 FLINK_DIST 代表flink 分布式jar包path
	 FLINK_CLASSPATH  代表其他的path


➜  ~
COMMENT
    while read -d '' -r jarfile ; do
    	# =~后面接正则表达式，这里在进行模式匹配，注意=~后面的正则表达式，不能加引号，如果使用变量，在变量赋值的时候可以加，但在if条件里面也不能加
    	#参考：https://www.cnblogs.com/gaochsh/p/6901807.html
        if [[ "$jarfile" =~ .*/flink-dist[^/]*.jar$ ]]; then
            FLINK_DIST="$FLINK_DIST":"$jarfile"
        elif [[ "$FLINK_CLASSPATH" == "" ]]; then
            FLINK_CLASSPATH="$jarfile";
        else
            FLINK_CLASSPATH="$FLINK_CLASSPATH":"$jarfile"
        fi
    done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    #检查是否有分布式jar包，没有就退出
    if [[ "$FLINK_DIST" == "" ]]; then
    	#>&2代表标准输出？
    	#linux中有三类文件是一直打开的，标准输入(0)、标准输出(1)、错误输出(2)，这里>&2，意思是把后面的内容输出到错误输出中。
    	#那这里有个问题了，我输出到标准输出和错误输出有什么区别?我看最终都显示到了控制台了，看起来似乎一样
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

		#这种情况下就退出
        # exit function with empty classpath to force process failure
        exit 1
    fi

	#通过echo 的方式返回方法值
    echo "$FLINK_CLASSPATH""$FLINK_DIST"
}

<<COMMENT
这里找出flink 分布式jar包
COMMENT
findFlinkDistJar() {
	#local是什么？local在函数中定义变量，用来约束变量作用域，将变量作用域固定到函数内部；
	# 如果不用local定义，函数中定义的变量作用域是从调用该函数开始到shell结束或变量删除的地方为止 ;
	# 参考：https://blog.csdn.net/wangjianno2/article/details/50200617
	# 这里定义了一个作用域在函数内部的局部变量FLINK_DIST，
	# 把lib目录下的flink-dist前缀的jar包找出来，赋值给FLINK_DIST,
	# 这里找出来的都是每个jar的绝对路径，一行是一个路径，类似：
	#/Users/linchen/work/kafka_2.5/kafka/core/build/distributions/kafka_2.13-2.8.0-SNAPSHOT/libs/connect-file-2.8.0-SNAPSHOT.jar
	#/Users/linchen/work/kafka_2.5/kafka/core/build/distributions/kafka_2.13-2.8.0-SNAPSHOT/libs/javassist-3.26.0-GA.jar
	#/Users/linchen/work/kafka_2.5/kafka/core/build/distributions/kafka_2.13-2.8.0-SNAPSHOT/libs/jakarta.validation-api-2.0.2.jar
	#/Users/linchen/work/kafka_2.5/kafka/core/build/distributions/kafka_2.13-2.8.0-SNAPSHOT/libs/jackson-module-jaxb-annotations-2.10.5.jar
	#/Users/linchen/work/kafka_2.5/kafka/core/build/distributions/kafka_2.13-2.8.0-SNAPSHOT/libs/commons-lang3-3.8.1.jar
    local FLINK_DIST="`find "$FLINK_LIB_DIR" -name 'flink-dist*.jar'`"

	#如果没有分布式jar包，那么久报错退出
    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi


	#最后还是以echo的方式返回值，我发现shell的函数返回值，一般都是用echo方式
    echo "$FLINK_DIST"
}

# These are used to mangle paths that are passed to java when using
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w "$1"`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/flink-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/flink-conf.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_LOG_MAX=10                              # Maximum number of old log files to keep
DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_JAVA_OPTS_JM=""                         # Optional JVM args (JobManager)
DEFAULT_ENV_JAVA_OPTS_TM=""                         # Optional JVM args (TaskManager)
DEFAULT_ENV_JAVA_OPTS_HS=""                         # Optional JVM args (HistoryServer)
DEFAULT_ENV_JAVA_OPTS_CLI=""                        # Optional JVM args (Client)
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode
DEFAULT_YARN_CONF_DIR=""                            # YARN Configuration Directory, if necessary
DEFAULT_HADOOP_CONF_DIR=""                          # Hadoop Configuration Directory, if necessary
DEFAULT_HBASE_CONF_DIR=""                           # HBase Configuration Directory, if necessary

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/flink-conf.yaml
########################################################################################################################

KEY_TASKM_COMPUTE_NUMA="taskmanager.compute.numa"

KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_LOG_DIR="env.log.dir"
KEY_ENV_LOG_MAX="env.log.max"
KEY_ENV_YARN_CONF_DIR="env.yarn.conf.dir"
KEY_ENV_HADOOP_CONF_DIR="env.hadoop.conf.dir"
KEY_ENV_HBASE_CONF_DIR="env.hbase.conf.dir"
KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts"
KEY_ENV_JAVA_OPTS_JM="env.java.opts.jobmanager"
KEY_ENV_JAVA_OPTS_TM="env.java.opts.taskmanager"
KEY_ENV_JAVA_OPTS_HS="env.java.opts.historyserver"
KEY_ENV_JAVA_OPTS_CLI="env.java.opts.client"
KEY_ENV_SSH_OPTS="env.ssh.opts"
KEY_HIGH_AVAILABILITY="high-availability"
KEY_ZK_HEAP_MB="zookeeper.heap.mb"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path and resolve directory symlinks
bin=`dirname "$target"`
SYMLINK_RESOLVED_BIN=`cd "$bin"; pwd -P`

# Define the main directory of the flink installation
# If config.sh is called by pyflink-shell.sh in python bin directory(pip installed), then do not need to set the FLINK_HOME here.
if [ -z "$_FLINK_HOME_DETERMINED" ]; then
    FLINK_HOME=`dirname "$SYMLINK_RESOLVED_BIN"`
fi
FLINK_LIB_DIR=$FLINK_HOME/lib
FLINK_PLUGINS_DIR=$FLINK_HOME/plugins
FLINK_OPT_DIR=$FLINK_HOME/opt


# These need to be mangled because they are directly passed to java.
# The above lib path is used by the shell script to retrieve jars in a
# directory, so it needs to be unmangled.
FLINK_HOME_DIR_MANGLED=`manglePath "$FLINK_HOME"`
if [ -z "$FLINK_CONF_DIR" ]; then FLINK_CONF_DIR=$FLINK_HOME_DIR_MANGLED/conf; fi
FLINK_BIN_DIR=$FLINK_HOME_DIR_MANGLED/bin
DEFAULT_FLINK_LOG_DIR=$FLINK_HOME_DIR_MANGLED/log
FLINK_CONF_FILE="flink-conf.yaml"
YAML_CONF=${FLINK_CONF_DIR}/${FLINK_CONF_FILE}

### Exported environment variables ###
export FLINK_CONF_DIR
export FLINK_BIN_DIR
export FLINK_PLUGINS_DIR
# export /lib dir to access it during deployment of the Yarn staging files
export FLINK_LIB_DIR
# export /opt dir to access it for the SQL client
export FLINK_OPT_DIR

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME=${JAVA_HOME}
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Flink config ./conf/flink-conf.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${MY_JAVA_HOME}
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d $JAVA_HOME ]]; then
        JAVA_RUN=$JAVA_HOME/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

IS_NUMBER="^[0-9]+$"

# Verify that NUMA tooling is available
command -v numactl >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    FLINK_TM_COMPUTE_NUMA="false"
else
    # Define FLINK_TM_COMPUTE_NUMA if it is not already set
    if [ -z "${FLINK_TM_COMPUTE_NUMA}" ]; then
        FLINK_TM_COMPUTE_NUMA=$(readFromConfig ${KEY_TASKM_COMPUTE_NUMA} "false" "${YAML_CONF}")
    fi
fi

if [ -z "${MAX_LOG_FILE_NUMBER}" ]; then
    MAX_LOG_FILE_NUMBER=$(readFromConfig ${KEY_ENV_LOG_MAX} ${DEFAULT_ENV_LOG_MAX} "${YAML_CONF}")
    export MAX_LOG_FILE_NUMBER
fi

if [ -z "${FLINK_LOG_DIR}" ]; then
    FLINK_LOG_DIR=$(readFromConfig ${KEY_ENV_LOG_DIR} "${DEFAULT_FLINK_LOG_DIR}" "${YAML_CONF}")
fi

if [ -z "${YARN_CONF_DIR}" ]; then
    YARN_CONF_DIR=$(readFromConfig ${KEY_ENV_YARN_CONF_DIR} "${DEFAULT_YARN_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
    HADOOP_CONF_DIR=$(readFromConfig ${KEY_ENV_HADOOP_CONF_DIR} "${DEFAULT_HADOOP_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${HBASE_CONF_DIR}" ]; then
    HBASE_CONF_DIR=$(readFromConfig ${KEY_ENV_HBASE_CONF_DIR} "${DEFAULT_HBASE_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_PID_DIR}" ]; then
    FLINK_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_ENV_JAVA_OPTS}" ]; then
    FLINK_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "${DEFAULT_ENV_JAVA_OPTS}" "${YAML_CONF}")

    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS="$( echo "${FLINK_ENV_JAVA_OPTS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_JM}" ]; then
    FLINK_ENV_JAVA_OPTS_JM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_JM} "${DEFAULT_ENV_JAVA_OPTS_JM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_JM="$( echo "${FLINK_ENV_JAVA_OPTS_JM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
    FLINK_ENV_JAVA_OPTS_TM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_TM} "${DEFAULT_ENV_JAVA_OPTS_TM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_TM="$( echo "${FLINK_ENV_JAVA_OPTS_TM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_HS}" ]; then
    FLINK_ENV_JAVA_OPTS_HS=$(readFromConfig ${KEY_ENV_JAVA_OPTS_HS} "${DEFAULT_ENV_JAVA_OPTS_HS}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_HS="$( echo "${FLINK_ENV_JAVA_OPTS_HS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_CLI}" ]; then
    FLINK_ENV_JAVA_OPTS_CLI=$(readFromConfig ${KEY_ENV_JAVA_OPTS_CLI} "${DEFAULT_ENV_JAVA_OPTS_CLI}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_CLI="$( echo "${FLINK_ENV_JAVA_OPTS_CLI}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_SSH_OPTS}" ]; then
    FLINK_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

# Define ZK_HEAP if it is not already set
if [ -z "${ZK_HEAP}" ]; then
    ZK_HEAP=$(readFromConfig ${KEY_ZK_HEAP_MB} 0 "${YAML_CONF}")
fi

# High availability
if [ -z "${HIGH_AVAILABILITY}" ]; then
     HIGH_AVAILABILITY=$(readFromConfig ${KEY_HIGH_AVAILABILITY} "" "${YAML_CONF}")
     if [ -z "${HIGH_AVAILABILITY}" ]; then
        # Try deprecated value
        DEPRECATED_HA=$(readFromConfig "recovery.mode" "" "${YAML_CONF}")
        if [ -z "${DEPRECATED_HA}" ]; then
            HIGH_AVAILABILITY="none"
        elif [ ${DEPRECATED_HA} == "standalone" ]; then
            # Standalone is now 'none'
            HIGH_AVAILABILITY="none"
        else
            HIGH_AVAILABILITY=${DEPRECATED_HA}
        fi
     fi
fi

# Arguments for the JVM. Used for job and task manager JVMs.
# DO NOT USE FOR MEMORY SETTINGS! Use conf/flink-conf.yaml with keys
# JobManagerOptions#TOTAL_PROCESS_MEMORY and TaskManagerOptions#TOTAL_PROCESS_MEMORY for that!
if [ -z "${JVM_ARGS}" ]; then
    JVM_ARGS=""
fi

# Check if deprecated HADOOP_HOME is set, and specify config path to HADOOP_CONF_DIR if it's empty.
if [ -z "$HADOOP_CONF_DIR" ]; then
    if [ -n "$HADOOP_HOME" ]; then
        # HADOOP_HOME is set. Check if its a Hadoop 1.x or 2.x HADOOP_HOME path
        if [ -d "$HADOOP_HOME/conf" ]; then
            # It's Hadoop 1.x
            HADOOP_CONF_DIR="$HADOOP_HOME/conf"
        fi
        if [ -d "$HADOOP_HOME/etc/hadoop" ]; then
            # It's Hadoop 2.2+
            HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
        fi
    fi
fi

# if neither HADOOP_CONF_DIR nor HADOOP_CLASSPATH are set, use some common default (if available)
if [ -z "$HADOOP_CONF_DIR" ] && [ -z "$HADOOP_CLASSPATH" ]; then
    if [ -d "/etc/hadoop/conf" ]; then
        echo "Setting HADOOP_CONF_DIR=/etc/hadoop/conf because no HADOOP_CONF_DIR or HADOOP_CLASSPATH was set."
        HADOOP_CONF_DIR="/etc/hadoop/conf"
    fi
fi

# Check if deprecated HBASE_HOME is set, and specify config path to HBASE_CONF_DIR if it's empty.
if [ -z "$HBASE_CONF_DIR" ]; then
    if [ -n "$HBASE_HOME" ]; then
        # HBASE_HOME is set.
        if [ -d "$HBASE_HOME/conf" ]; then
            HBASE_CONF_DIR="$HBASE_HOME/conf"
        fi
    fi
fi

# try and set HBASE_CONF_DIR to some common default if it's not set
if [ -z "$HBASE_CONF_DIR" ]; then
    if [ -d "/etc/hbase/conf" ]; then
        echo "Setting HBASE_CONF_DIR=/etc/hbase/conf because no HBASE_CONF_DIR was set."
        HBASE_CONF_DIR="/etc/hbase/conf"
    fi
fi

INTERNAL_HADOOP_CLASSPATHS="${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}"

if [ -n "${HBASE_CONF_DIR}" ]; then
    INTERNAL_HADOOP_CLASSPATHS="${INTERNAL_HADOOP_CLASSPATHS}:${HBASE_CONF_DIR}"
fi

# Auxilliary function which extracts the name of host from a line which
# also potentially includes topology information and the taskManager type
extractHostName() {
    # handle comments: extract first part of string (before first # character)
    WORKER=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$WORKER" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            WORKER=${BASH_REMATCH[1]}
    fi

    echo $WORKER
}

readMasters() {
    MASTERS_FILE="${FLINK_CONF_DIR}/masters"

    if [[ ! -f "${MASTERS_FILE}" ]]; then
        echo "No masters file. Please specify masters in 'conf/masters'."
        exit 1
    fi

    MASTERS=()
    WEBUIPORTS=()

    MASTERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOSTWEBUIPORT=$( extractHostName $line)

        if [ -n "$HOSTWEBUIPORT" ]; then
            HOST=$(echo $HOSTWEBUIPORT | cut -f1 -d:)
            WEBUIPORT=$(echo $HOSTWEBUIPORT | cut -s -f2 -d:)
            MASTERS+=(${HOST})

            if [ -z "$WEBUIPORT" ]; then
                WEBUIPORTS+=(0)
            else
                WEBUIPORTS+=(${WEBUIPORT})
            fi

            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                MASTERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$MASTERS_FILE"
}

readWorkers() {
    WORKERS_FILE="${FLINK_CONF_DIR}/workers"

    if [[ ! -f "$WORKERS_FILE" ]]; then
        echo "No workers file. Please specify workers in 'conf/workers'."
        exit 1
    fi

    WORKERS=()

    WORKERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            WORKERS+=(${HOST})
            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                WORKERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$WORKERS_FILE"
}

# starts or stops TMs on all workers
# TMWorkers start|stop
TMWorkers() {
    CMD=$1

    readWorkers

    if [ ${WORKERS_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for worker in ${WORKERS[@]}; do
            "${FLINK_BIN_DIR}"/taskmanager.sh "${CMD}"
        done
    else
        # non-local setup
        # start/stop TaskManager instance(s) using pdsh (Parallel Distributed Shell) when available
        command -v pdsh >/dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            for worker in ${WORKERS[@]}; do
                ssh -n $FLINK_SSH_OPTS $worker -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\" &"
            done
        else
            PDSH_SSH_ARGS="" PDSH_SSH_ARGS_APPEND=$FLINK_SSH_OPTS pdsh -w $(IFS=, ; echo "${WORKERS[*]}") \
                "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\""
        fi
    fi
}

runBashJavaUtilsCmd() {
    local cmd=$1
    local conf_dir=$2
    local class_path=$3
    local dynamic_args=${@:4}
    class_path=`manglePathList "${class_path}"`

    local output=`${JAVA_RUN} -classpath "${class_path}" org.apache.flink.runtime.util.bash.BashJavaUtils ${cmd} --configDir "${conf_dir}" $dynamic_args 2>&1 | tail -n 1000`
    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Cannot run BashJavaUtils to execute command ${cmd}." 1>&2
        # Print the output in case the user redirect the log to console.
        echo "$output" 1>&2
        exit 1
    fi

    echo "$output"
}

extractExecutionResults() {
    local output="$1"
    local expected_lines="$2"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"
    local execution_results
    local num_lines

    execution_results=$(echo "${output}" | grep ${EXECUTION_PREFIX})
    num_lines=$(echo "${execution_results}" | wc -l)
    # explicit check for empty result, becuase if execution_results is empty, then wc returns 1
    if [[ -z ${execution_results} ]]; then
        echo "[ERROR] The execution result is empty." 1>&2
        exit 1
    fi
    if [[ ${num_lines} -ne ${expected_lines} ]]; then
        echo "[ERROR] The execution results has unexpected number of lines, expected: ${expected_lines}, actual: ${num_lines}." 1>&2
        echo "[ERROR] An execution result line is expected following the prefix '${EXECUTION_PREFIX}'" 1>&2
        echo "$output" 1>&2
        exit 1
    fi

    echo "${execution_results//${EXECUTION_PREFIX}/}"
}

extractLoggingOutputs() {
    local output="$1"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    echo "${output}" | grep -v ${EXECUTION_PREFIX}
}

parseJmArgsAndExportLogs() {
  java_utils_output=$(runBashJavaUtilsCmd GET_JM_RESOURCE_PARAMS "${FLINK_CONF_DIR}" "${FLINK_BIN_DIR}/bash-java-utils.jar:$(findFlinkDistJar)" "$@")
  logging_output=$(extractLoggingOutputs "${java_utils_output}")
  params_output=$(extractExecutionResults "${java_utils_output}" 2)

  if [[ $? -ne 0 ]]; then
    echo "[ERROR] Could not get JVM parameters and dynamic configurations properly."
    echo "[ERROR] Raw output from BashJavaUtils:"
    echo "$java_utils_output"
    exit 1
  fi

  jvm_params=$(echo "${params_output}" | head -n1)
  export JVM_ARGS="${JVM_ARGS} ${jvm_params}"
  export DYNAMIC_PARAMETERS=$(IFS=" " echo "${params_output}" | tail -n1)

  export FLINK_INHERITED_LOGS="
$FLINK_INHERITED_LOGS

JM_RESOURCE_PARAMS extraction logs:
jvm_params: $jvm_params
logs: $logging_output
"
}
