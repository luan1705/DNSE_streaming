#!/bin/bash

SRC="../SSI_streaming/streaming/List/exchange.py"

if [ ! -f "$SRC" ]; then
    echo "❌ Không tìm thấy file nguồn: $SRC"
    exit 1
fi

cp "$SRC" "./chart/List/exchange.py"
echo "✅ Synced exchange.py → chart/List/"

cp "$SRC" "./dashboard/List/exchange.py"
echo "✅ Synced exchange.py → dashboard/List/"