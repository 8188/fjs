#!/bin/sh
celery -A tasks purge -f
celery -A tasks worker -P eventlet -l warning -f logs/celery.log -c 4
