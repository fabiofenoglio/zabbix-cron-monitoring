#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "running cron wrapper [sh] from $DIR as $(whoami)"
python3 $DIR/cron-wrapper.py "$@"
exit $?