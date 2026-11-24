#!/bin/bash
# sync_to_github.sh — commita a cada lote e pusha para o GitHub.
set -u
cd /home/ubuntu/torbot-catalog || { echo "ERRO: diretorio nao encontrado"; exit 1; }

# 1) Adiciona os arquivos de interesse (outputs + scripts + source + .gitignore)
git add \
    catalog.csv results.json visited_urls.json \
    run_crawl.sh validate_crawl.sh fetch_query.py \
    dark-web-crawler-v3.py .gitignore sync_to_github.sh \
    tools crawl.log

# 2) Verifica se há mudancas stagadas
if git diff --cached --quiet; then
  echo "[sync] sem mudancas a commitar (tudo atualizado)."
  exit 0
fi

# 3) Commit
MSG="sync: batch update $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if ! git commit -m "$MSG" 2>&1; then
  echo "[sync] ERRO no commit" >&2
  exit 1
fi
echo "[sync] commit realizado: $MSG"

# 4) Push
if git push origin main 2>&1; then
  echo "[sync] push OK para origin/main"
else
  echo "[sync] ERRO no push" >&2
  exit 1
fi
