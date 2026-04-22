#!/usr/bin/env bash
# Install cpac-checking-data as a systemd timer job on Raspberry Pi.
#
# Usage (run as root, e.g. with sudo):
#   sudo SERVICE_USER=pi \
#        PROJECT_DIR=/home/pi/service-cpac-checking-data \
#        DB_PATH=/home/pi/service-cpac-send-iot/local_iot_data.db \
#        CLOUD_API_BASE_URL=https://6cq2hsx83h.execute-api.ap-southeast-1.amazonaws.com \
#        SIDE_ID=cpac-riverside \
#        ./install.sh

set -euo pipefail

SERVICE_USER="${SERVICE_USER:-pi}"
PROJECT_DIR="${PROJECT_DIR:-/home/pi/service-cpac-checking-data}"
DB_PATH="${DB_PATH:-/home/pi/service-cpac-send-iot/local_iot_data.db}"
CLOUD_API_BASE_URL="${CLOUD_API_BASE_URL:-https://6cq2hsx83h.execute-api.ap-southeast-1.amazonaws.com}"
SIDE_ID="${SIDE_ID:-cpac-riverside}"

UNIT_DIR="/etc/systemd/system"
SERVICE_SRC="$(dirname "$0")/cpac-checking-data.service"
TIMER_SRC="$(dirname "$0")/cpac-checking-data.timer"

if [[ ! -f "$SERVICE_SRC" || ! -f "$TIMER_SRC" ]]; then
  echo "❌ Could not find unit files next to install.sh"
  exit 1
fi

# venv check
if [[ ! -x "${PROJECT_DIR}/.venv/bin/python" ]]; then
  echo "⚠️  No venv at ${PROJECT_DIR}/.venv — creating one..."
  python3 -m venv "${PROJECT_DIR}/.venv"
fi

echo "📝 Rendering unit files to ${UNIT_DIR}"

sed \
  -e "s|^User=.*|User=${SERVICE_USER}|" \
  -e "s|^Group=.*|Group=${SERVICE_USER}|" \
  -e "s|^WorkingDirectory=.*|WorkingDirectory=${PROJECT_DIR}|" \
  -e "s|^ExecStart=.*|ExecStart=${PROJECT_DIR}/.venv/bin/python ${PROJECT_DIR}/app.py|" \
  -e "s|^Environment=LOCAL_DB_PATH=.*|Environment=LOCAL_DB_PATH=${DB_PATH}|" \
  -e "s|^Environment=CLOUD_API_BASE_URL=.*|Environment=CLOUD_API_BASE_URL=${CLOUD_API_BASE_URL}|" \
  -e "s|^Environment=CPAC_SIDE_ID=.*|Environment=CPAC_SIDE_ID=${SIDE_ID}|" \
  "$SERVICE_SRC" > "${UNIT_DIR}/cpac-checking-data.service"

cp "$TIMER_SRC" "${UNIT_DIR}/cpac-checking-data.timer"

chmod 644 "${UNIT_DIR}/cpac-checking-data.service" "${UNIT_DIR}/cpac-checking-data.timer"

systemctl daemon-reload
systemctl enable --now cpac-checking-data.timer

echo "✅ Installed."
echo ""
echo "Useful commands:"
echo "  systemctl status  cpac-checking-data.timer"
echo "  systemctl status  cpac-checking-data.service"
echo "  systemctl start   cpac-checking-data.service   # run once right now"
echo "  journalctl -u cpac-checking-data.service -f    # follow logs"
echo "  systemctl list-timers cpac-checking-data.timer"
