#!/usr/bin/env bash
# Clones the versions of each framework that results.txt was measured on.
FW=${FW:-/root/fw}
mkdir -p "$FW"
get() {
  [ -d "$FW/$2" ] || git -c advice.detachedHead=false clone -q --depth 1 --branch "$3" "https://github.com/$1/$2" "$FW/$2"
}
case ${1:-all} in
  ruby | all) get sidekiq sidekiq v8.1.7; get rails rails v8.1.4.1 ;;&
  python | all) get celery celery v5.7.0; get celery kombu v5.7.0 ;;&
  php | all) get laravel framework v13.40.0 ;;&
  node | all) get taskforcesh bullmq v6.5.4 ;;
esac
