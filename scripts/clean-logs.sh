#!/usr/bin/env bash
set -euo pipefail

# 1. Rotate and clean Nginx logs in tmp/nginx/
NGINX_LOG_DIR="/home/ubuntu/sr_web_server/tmp/nginx"
DATE_SUFFIX=$(date +%Y%m%d)

echo "=== Log cleaning started at $(date) ==="

if [ -d "${NGINX_LOG_DIR}" ]; then
  echo "Rotating Nginx logs..."
  for logfile in "${NGINX_LOG_DIR}"/*.log; do
    if [ -f "${logfile}" ]; then
      echo "Rotating: ${logfile}"
      mv "${logfile}" "${logfile}.${DATE_SUFFIX}"
    fi
  done
  
  # Signal Nginx to reopen log files
  if [ -f "${NGINX_LOG_DIR}/nginx.pid" ]; then
    echo "Signaling Nginx to reopen logs..."
    nginx -p /home/ubuntu/sr_web_server -c /home/ubuntu/sr_web_server/nginx.conf -s reopen || true
  fi

  # Delete Nginx rotated logs older than 2 days (i.e. modified more than 1 day ago / mtime +1)
  echo "Cleaning Nginx logs older than 2 days..."
  find "${NGINX_LOG_DIR}" -name "*.log.*" -type f -mtime +1 -print -delete
else
  echo "Nginx log directory not found: ${NGINX_LOG_DIR}"
fi

# 2. Rotate and clean PM2 logs in /home/ubuntu/.pm2/logs
PM2_LOG_DIR="/home/ubuntu/.pm2/logs"

if [ -d "${PM2_LOG_DIR}" ]; then
  echo "Rotating PM2 logs..."
  for logfile in "${PM2_LOG_DIR}"/*.log; do
    if [ -f "${logfile}" ]; then
      echo "Rotating: ${logfile}"
      mv "${logfile}" "${logfile}.${DATE_SUFFIX}"
    fi
  done

  # Signal PM2 to reopen log files
  echo "Signaling PM2 to reload logs..."
  npx pm2 reloadLogs || pm2 reloadLogs || true

  # Delete PM2 rotated logs older than 2 days
  echo "Cleaning PM2 logs older than 2 days..."
  find "${PM2_LOG_DIR}" -name "*.log.*" -type f -mtime +1 -print -delete
else
  echo "PM2 log directory not found: ${PM2_LOG_DIR}"
fi

echo "=== Log cleaning completed successfully ==="
