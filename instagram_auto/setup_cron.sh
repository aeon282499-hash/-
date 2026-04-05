#!/bin/bash
# cronジョブのセットアップスクリプト
# 毎朝9:00にオーケストレーターを自動実行する

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_PATH="$(which python3)"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "${LOG_DIR}"

# cronエントリーの定義
CRON_ENTRY="0 9 * * * cd ${SCRIPT_DIR} && ${PYTHON_PATH} orchestrator.py >> ${LOG_DIR}/cron.log 2>&1"

# 既存のエントリーを確認して重複登録を防ぐ
EXISTING=$(crontab -l 2>/dev/null | grep "orchestrator.py")

if [ -n "${EXISTING}" ]; then
    echo "cronジョブはすでに登録されています："
    echo "${EXISTING}"
else
    # cronジョブを追加
    (crontab -l 2>/dev/null; echo "${CRON_ENTRY}") | crontab -
    echo "cronジョブを登録しました："
    echo "${CRON_ENTRY}"
fi

echo ""
echo "現在のcrontab："
crontab -l
