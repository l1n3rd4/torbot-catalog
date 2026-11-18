# torbot-catalog

Repositório de trabalho para o monitoramento de sites .onion via Tor.

- `dark-web-crawler-v2.py` — crawler BFS (MAX_CHILDREN=50, dedup via visited_urls.json)
- `extrair_brasileiro.py` — extração de dados do crawl
- `catalog.csv` — lista de domínios monitorados
- `torbot-crawler.service` — unidade do systemd (opcional)

## Estrutura de dados
- `catalog.csv` — máster de domínios
- `visited_urls.json` — páginas já visitadas (evita re-crawling)
