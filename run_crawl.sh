#!/bin/bash
# Corredor contínuo do Torbot catalog: roda lotes de 25 domínios de 1 em 1,
# continuando onde parou (visited_urls.json). Para quando todos os domínios acabarem.
cd /home/ubuntu/torbot-catalog || exit 1
while true; do
  vis=$(python3 -c "import json;print(len(json.load(open('visited_urls.json'))))" 2>/dev/null || echo 0)
  if [ "$vis" -ge 11421 ]; then
    echo "$(date -u +%FT%TZ) ALL DONE (visited=$vis)" >> crawl.log
    echo "ALL DONE (visited=$vis)"
    break
  fi
  /home/ubuntu/tools/TorBot/.venv/bin/python dark-web-crawler-v3.py --fast --concurrent 4 --limit 25 >> crawl.log 2>&1
  sleep 15
done
