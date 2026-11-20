# torbot-catalog

Repositório de trabalho para monitoramento de sites `.onion` via Tor, usando as
funcionalidades do **TorBot** sem reescrevê-lo.

## Arquitetura

- **Fetch via túnel SOCKS5** (`dark-web-crawler-v3.py`): a função `tor_fetch()`
  replica exatamente o que o `curl --socks5-hostname 127.0.0.1:9050` faz:
  handshake SOCKS5 → autenticação vazia → CONNECT com DNS remoto. Isso corrige o
  problema que o `httpx 0.27` (`proxy=socks5://`) não resolve `.onion` localmente
  e não envia o auth SOCKS5 que o Tor exige.
- **Trobot** (`/home/ubuntu/tools/TorBot`) é usado para: a árvore de links
  (`LinkTree`, utilizada nos *hidden wiki* para percorrer `children` +
  `filhos de filhos`, profundidade 2), as categorias (`categorize`), o
  classificador de idiomas e `CrawlResultAdapter`.
- **Categorias simples (5):** `streaming`, `wiki`, `fórum/chat`, `marketplace`,
  `generic`. Idioma: `pt` / `en` (heurísticas puros, 0% IA).

## Solo incrementais

- **Sem threads** no modo incremental (1 domínio por vez).
- **Incremental:** `visited_urls.json` guarda o que já foi visitado; cada corrida
  só processa o que é novo. Progresso salvo periodicamente.
- Depois dos 10 domínios iniciais, activa-se a concorrência com
  `ThreadPoolExecutor` (opção `--concurrent N`).

## Executar

```bash
# modo incremental (seqüencial, 1 thread)
python3 dark-web-crawler-v3.py --fast

# concorrência (após os 10 iniciais)
python3 dark-web-crawler-v3.py --fast --concurrent 4
```

Todos os outputs (`catalog.csv`, `visited_urls.json`, `results.json`,
`analysis_report.json`) ficam no diretório do repositório (ignorados por
`.gitignore`).

## Correção do Tor (curl vs python)

- `curl --socks5-hostname 127.0.0.1:9050` consegue ligar-se a serviços `.onion`
  porque resolve o hostname no proxy (DNS remoto) e envia a autenticação SOCKS5
  vazia.
- O `httpx 0.27` (`socks5://`) resolve o DNS localmente e não envia o auth.
  Por isso `.onion` falha.
- Solução: `tor_fetch()` tenta o túnel em Python (coração da correção); se o
  Tor fechar a stream (O.S. / ciclo de vida do proxy tor), recua para a chamada de
  `curl` — o que garante que o script segue funcionando.
