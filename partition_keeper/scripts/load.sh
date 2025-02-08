#!/bin/sh

execute_from_multi_model_eval=0
if [ $# -gt 1 ]; then
  execute_from_multi_model_eval=$2
fi

cd `dirname $0` || exit 
retry_times=600
dev_shm_dirs=""
absolute_path=`readlink -f .`
service_dir=`dirname $absolute_path`
service_name=`basename $service_dir`
monitor_server="reco.monitor.internal"
tools_path=$service_dir/tools
log_path=$service_dir/log
supervise_path=$service_dir/bin/supervise
root_path=`echo $service_dir | awk -F \/ '{print $2}'`
if [[ $root_path = "data" ]] || [[ $root_path = "media" ]]; then supervisord_path=/data/web_server/supervisord; else supervisord_path=/home/web_server/supervisord;  fi

function load_config() {
  full_path=`pwd`/$0
  dir_name=`dirname $0`
  config_file=$dir_name/"load_config"
  if [[ -f $config_file ]]; then
    echo "source $config_file"
    source $config_file
  fi

  ksp_export_file=$service_dir/ksp-export.sh
  if [[ -f $ksp_export_file ]]; then
    echo "source $ksp_export_file"
    source $ksp_export_file
  fi
}

function link_log_dir() {
  home_dir=`readlink -f ~`
  expected_log_dir=$home_dir/logs/$service_name
  if [ ! -d $expected_log_dir ]; then
    unlink $expected_log_dir
    mkdir -p $expected_log_dir
  fi
  if [ ! -d $expected_log_dir ]; then
    echo "failed create log dir $expected_log_dir"
    return
  fi
  abs_log_dir=`readlink -f $log_path`
  if [ $expected_log_dir != $abs_log_dir ]; then
    mv $log_path/* $expected_log_dir/
    rm -rf $log_path
    ln -sf $expected_log_dir $log_path
  fi
}

function modify_supervisor_conf() {
  filedir=$1
  filename=$2
  # 修改command
  supervisor_conf=$tools_path/$filedir/$filename
  
  supervisor_command="command ="`awk -F"command =" '{print $2}' $supervisor_conf`
  supervisor_command_path=$tools_path/$filedir/bin/run.sh
  new_supervisor_command="command = `echo $supervisor_command_path` $tools_path/$filedir/bin $service_name $monitor_server"
  sed -i "s#`echo $supervisor_command`#`echo $new_supervisor_command`#"  $supervisor_conf

  # 修改logerr
  supervisor_logerr="stderr_logfile ="`awk -F "stderr_logfile =" '{print $2}' $supervisor_conf`
  supervisor_logerr_path=$tools_path/$filedir/logs/stderr.log
  new_supervisor_logerr="stderr_logfile = `echo $supervisor_logerr_path`"
  sed -i "s#`echo $supervisor_logerr`#`echo $new_supervisor_logerr`#"  $supervisor_conf
  
# 修改logout
  supervisor_logout="stdout_logfile ="`awk -F "stdout_logfile =" '{print $2}' $supervisor_conf`
  supervisor_logout_path=$tools_path/$filedir/logs/stdout.log
  new_supervisor_logout="stdout_logfile = `echo $supervisor_logout_path`"
  sed -i "s#`echo $supervisor_logout`#`echo $new_supervisor_logout`#"  $supervisor_conf

# 修改dir
  supervisor_dir="directory ="`awk -F "directory =" '{print $2}' $supervisor_conf`
  supervisor_dir_path=$tools_path/$filedir/logs/
  new_supervisor_dir="directory = `echo $supervisor_dir_path`"
  sed -i "s#`echo $supervisor_dir`#`echo $new_supervisor_dir`#"  $supervisor_conf

# 修改logfile
  supervisor_logfile="logfile ="`awk -F "^logfile =" '{print $2}' $supervisor_conf`
  supervisor_logfile_path=$tools_path/$filedir/logs/log.log
  new_supervisor_logfile="logfile = `echo $supervisor_logfile_path`"
  sed -i "s#`echo $supervisor_logfile`#`echo $new_supervisor_logfile`#"  $supervisor_conf

}

function start_supervisor() {
  if [ -f $supervisord_path/conf/conf.d/log-collector.conf ]; then
    rm $supervisord_path/conf/conf.d/log-collector.conf
  fi
  if [ -f $supervisord_path/conf/conf.d/core-collector.conf ]; then
    rm $supervisord_path/conf/conf.d/core-collector.conf
  fi

  if [[ $service_name == test_* ]] || [[ $service_name == *_test ]] || [[ $service_name == dryrun_* ]] || [[ $service_name == *_dryrun ]]; then
    return
  fi

  if [ -d "$tools_path/log-collector" ]; then
    modify_supervisor_conf log-collector log-collector.conf 
    cp $tools_path/log-collector/log-collector.conf  $supervisord_path/conf/conf.d/
  fi
  
  if [ -d "$tools_path/core-collector" ]; then
    modify_supervisor_conf core-collector core-collector.conf 
    cp $tools_path/core-collector/core-collector.conf  $supervisord_path/conf/conf.d/
  fi

  if [[ -d "$tools_path/log-collector" ]] || [[ -d "$tools_path/core-collector" ]]; then
    echo "updating supervisor_conf...."

    supervisord_ret=`ps aux | grep -E '/usr/bin/supervisord|/bin/supervisord' | grep  'web_server/supervisord/conf/supervisord.conf' | grep -v 'grep' | wc -l`
    if [[ ${supervisord_ret} -ne 1 ]]; then
      supervisord -c $supervisord_path/conf/supervisord.conf
    fi

    supervisorctl -c $supervisord_path/conf/supervisord.conf update
    supervisorctl -c $supervisord_path/conf/supervisord.conf reload
    supervisorctl -c $supervisord_path/conf/supervisord.conf start all
  fi
}

function clear_dev_shm_directory() {
  for d in $@
  do
    if [ -d $d ] ; then 
      rm -rf $d
    fi
  done
}

function clear_supervise_directory() {
  if [ -d $supervise_path ] ; then 
    rm -rf $supervise_path
  fi
}

function start_service() {
  ulimit -c unlimited
  if [ "$DEPLOY_LINK_LOG_DIR" = true ]; then
    echo "try link log dir"
    link_log_dir
  elif [ "$ksp_link_log_dir" = true ]; then
    echo "ksp link log dir [lower case]"
    link_log_dir
  elif [ "$KSP_LINK_LOG_DIR" = true ]; then
    echo "ksp link log dir [upper case]"
    link_log_dir
  fi
  start_supervisor
  if [ $? -ne 0 ]; then
    echo "start_supervisor failed, status:" $?
  fi
  if [ -f  $log_path/server_run.out ]; then
    echo "backup server_run...."
    datetime=`date +%Y%m%d-%H%M%S`
    mv $log_path/server_run.out $log_path/server_run.out.`echo $datetime`
  fi
  clear_dev_shm_directory $dev_shm_dirs
  clear_dev_shm_directory $krp_clean_shm_dirs
  clear_supervise_directory
  echo "starting service...."
  ../supervise $absolute_path 0</dev/null &>/dev/null &
  if [ $? -ne 0 ]; then
    echo "starting service failed, status:" $?
  fi
  if [ -f ../bin/multi_model_evaluate.py -a -f ../config/dynamic_json_config.json -a "$execute_from_multi_model_eval" == "0" ]; then
    if grep multi_model_eval ../config/dynamic_json_config.json >/dev/null 2>&1; then
      nohup python3 ../bin/multi_model_evaluate.py >../log/multi_model_evaluate.log 2>&1 &
    fi
  fi
}

function kill_pid() {
  pid=$1
  kill_times=0
  local retry_times=$(( $2 ))
  if [ $retry_times -le 0 ]; then
    retry_times=600
  fi
  until [[ -z `ps aux | awk '{print $2}' | grep "^$pid$"` ]]; do
    let kill_times=kill_times+1
    if [[ $kill_times -gt $retry_times ]]; then
      echo "'kill $pid' time out, use 'kill -9 $pid' to kill it"
      kill -9 $pid;
      until [[ -z `ps aux | awk '{print $2}' | grep -v grep | grep "^$pid$"` ]]; do
        sleep 1s
      done 
      return 0;
    else
      kill $pid
    fi
    sleep 1s
  done;
}

function stop_supervisor() {
  log_collector_pid=`ps aux | sed "s/web_server\///" | grep "$tools_path/log-collector" | awk '!/grep/ {print $2}'`
  if [ -n "$log_collector_pid" ]; then
    echo "stoping log_collector...."
    supervisorctl -c $supervisord_path/conf/supervisord.conf stop log-collector
  fi
  core_collector_pid=`ps aux | sed "s/web_server\///" | grep "$tools_path/core-collector" | awk '!/grep/ {print $2}'`
  if [ -n "$core_collector_pid" ]; then
    echo "stoping core_collector...."
    supervisorctl -c $supervisord_path/conf/supervisord.conf stop core-collector
  fi
}

function stop_service() {
  short_path=`echo $absolute_path | sed "s/web_server\///"`
  if [ "$execute_from_multi_model_eval" == "0" ]; then
    evaluate_pids=`ps aux|grep multi_model_evaluate.py|grep -v grep|awk '{print $2}'`
    for evaluate_pid in ${evaluate_pids[@]};
    do
      if [ -n "$evaluate_pid" ]; then
        echo "kill multi_model_evaluate.py: $evaluate_pid ..."
        kill_pid $evaluate_pid $1
      fi
    done
  fi
  supervise_pids=`ps aux | sed "s/web_server\///" | grep 'supervise' | grep "$short_path" | awk '!/grep/ {print $2}'`
  if [ -n "$supervise_pids" ]; then
    for supervise_pid in ${supervise_pids[@]}
    do
      echo "kill supervise pid: $supervise_pid ..."
      kill_pid $supervise_pid $1
    done
  fi
  
  server_pids=`ps aux | sed "s/web_server\///" | grep "$short_path/$service_name" | awk '!/grep/ {print $2}'`;
  for server_pid in ${server_pids[@]};
  do
    if [ -n "$server_pid" ]; then
      echo "kill server: $server_pid ..."
      kill_pid $server_pid $1
    fi
  done
  
 
  stop_supervisor
  if [ $? -ne 0 ]; then
    echo "stop_supervisor failed, status:" $?
  fi

  return 0;
}

function stop_diag_opt_service() {
  short_path=`echo $absolute_path | sed "s/web_server\///"`
  if [ "$execute_from_multi_model_eval" == "0" ]; then
    evaluate_pids=`ps aux|grep multi_model_evaluate.py|grep -v grep|awk '{print $2}'`
    for evaluate_pid in ${evaluate_pids[@]};
    do
      if [ -n "$evaluate_pid" ]; then
        echo "kill multi_model_evaluate.py: $evaluate_pid ..."
        kill_pid $evaluate_pid $1
      fi
    done
  fi
  supervise_pids=`ps aux | sed "s/web_server\///" | grep 'supervise' | grep "$short_path" | awk '!/grep/ {print $2}'`
  if [ -n "$supervise_pids" ]; then
    for supervise_pid in ${supervise_pids[@]}
    do
      echo "kill supervise pid: $supervise_pid ..."
      kill_pid $supervise_pid 1
    done
  fi
  
  server_pids=`ps aux | sed "s/web_server\///" | grep "$short_path/$service_name" | awk '!/grep/ {print $2}'`;
  for server_pid in ${server_pids[@]};
  do
    if [ -n "$server_pid" ]; then
      echo "kill server: $server_pid ..."
      kill $server_pid
    fi
  done
 
  stop_supervisor 
  if [ $? -ne 0 ]; then
    echo "stop_supervisor failed, status:" $?
  fi


  return 0;
}


load_config

case $1 in
start|load)
  start_service
  ;;
reload|restart)
  stop_service $retry_times
  stop_service $retry_times
  stop_service $retry_times
  sleep 20s
  start_service
  ;;
stop)
  stop_service $retry_times
  ;;
stop_diag_opt)
  stop_diag_opt_service
  ;;
load_config)
  set -x
  load_config
  ;;
*)
  echo "usage: $0 start|load|stop|reload|restart"
  ;;
esac
