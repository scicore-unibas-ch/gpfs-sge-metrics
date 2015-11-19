#!/bin/bash

# I execute this script in crontab every minute

source /etc/profile.d/sge.sh

python /opt/scicore-monitoring/sge-stats-influxdb.py &> /dev/null
