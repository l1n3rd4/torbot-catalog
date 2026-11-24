#!/bin/bash
# Validação periódica do crawler Torbot catalog.
# Relatórios: visitados, sucesso/erro, taxa de download, lote ativo.
cd /home/ubuntu/torbot-catalog || exit 1

vis=$(python3 -c "import json;print(len(json.load(open('visited_urls.json'))))" 2>/dev/null || echo 0)
total=$(grep -oE '[a-z2-7]{56}\.onion' /home/ubuntu/ahmia_index.html | sort -u | wc -l)

cat <<EOF
=== STATUS TORBOT-CATALOG ===
[$(date -u +%FT%TZ)]
Visitados: $vis / $total (faltam $(( total - vis )) para mapear)
EOF

# taxa de download (domínios/6h)
if [ -f crawl.log ]; then
  last_success=$(tail -n 50 crawl.log | grep -c '✓' 2>/dev/null)
  last_err=$(tail -n 50 crawl.log | grep -c '✗' 2>/dev/null)
  echo "Últimos 50 logs: $last_success ok / $last_err falha"
fi

# status do batch ativo
if [ -f results.json ]; then
  python3 -c "
import json
try:
    d=json.load(open('results.json'))
    print('batch atual:', len(d), 'registros |', sum(1 for r in d if r.get('status')=='success'), 'sucesso |', sum(1 for r in d if r.get('status')=='error'), 'erro')
except Exception as e:
    print('results.json:', e)
"
fi

echo "=== Pentimento (últimos erros) ==="
tail -n 50 crawl.log | grep "✗" | tail -5
