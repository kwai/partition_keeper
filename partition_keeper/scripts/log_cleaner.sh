#!/bin/bash

set -u

watch_dir=""
kept_count=10
run_once=false

help() {
   echo "log_cleaner.sh"
   echo "  -watch_dir <dir>"
   echo "  -kept_count <count>"
   exit 1
} >&2 

parse_flags() {
   while [ $# -ge 1 ]; do
      case $1 in
         -watch_dir)
            shift
            watch_dir=$1
            ;;
         -kept_count)
            shift
            kept_count=$1
            ;;
         -run_once)
            run_once=true
            ;;
         *)
            help
            ;;
      esac
      shift
   done

   if [ x$watch_dir == "x" ]; then
      help
   fi
   if [ $kept_count -lt 3 ]; then
      kept_count=3
   fi
}

remove_useless() {
   PATTERN=$1
   pushd $watch_dir > /dev/null 2>&1
   count=`find . -name "*$PATTERN*" | grep -v "$PATTERN.slog" | sort | wc -l`
   if [ $count -gt $kept_count ]; then
      removed_count=$[count - kept_count]
      find . -name "*$PATTERN*" | grep -v "$PATTERN.slog" | sort | head -n $removed_count | xargs -I {} rm -rf {}
   fi
   popd > /dev/null 2>&1
}

run() {
   while :; do
      remove_useless INFO
      remove_useless WARNING
      remove_useless ERROR
      if [ $run_once == "true" ]; then
          break
      else
         sleep 10
      fi
   done
}

parse_flags $@
run
