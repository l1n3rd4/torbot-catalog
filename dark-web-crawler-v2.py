#!/usr/bin/env python3
"""
dark-web-crawler-v2.py — Catálogo de sites .onion via Ahmia + Trobot (httpx).

Novidades:
  1. Follow de links internos: extrai <a href> de cada página e segue até
     MAX_CHILDREN por domínio (BFS limitado).
  2. Armazenamento com hashmap: VISITED_URLS (set) evita salvamentos duplicados.
  3. Crawler pesado via Trobot (httpx 0.27 + socksio) em vez de curl subprocess.
  4. Threads: 4 workers padrão.

Uso:
    /home/ubuntu/tools/TorBot/.venv/bin/python dark-web-crawler-v2.py               # modo interativo
    --fast            # só novos (pula URLs já na CSV)
    --quiet           # sem logs interativos
    --threads N       # batch de N threads
    --max-children N  # máx. links filhos por site (0 = sem follow)
    --limit N         # limite de domínios iniciais

Requisitos:
    /home/ubuntu/tools/TorBot/.venv/bin/python dark-web-crawler-v2.py

Dependências (no venv do Trobot):
    httpx 0.27.0, beautifulsoup4 4.12.3, socksio 1.0.0
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import deque
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Trobot / dependencies ───────────────────────────────────────────────────
try:
    import httpx
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"[ERRO] Dependência faltando: {e}", flush=True)
    sys.exit(1)

# ── Configuração global ─────────────────────────────────────────────────────
TOR_SOCKS_PROXY = "socks5://127.0.0.1:9050"   # httpx 0.27 só aceita socks5://
AHMI_INDEX_URL = "https://ahmia.fi/onions/"
OUTPUT_DIR = os.path.expanduser("~/torbot-catalog")
CSV_PATH = os.path.join(OUTPUT_DIR, "catalog.csv")
META_JSON = os.path.join(OUTPUT_DIR, "results.json")
TIMEOUT = 15
THREADS = 4
MAX_CHILDREN_PER_SITE = 50          # 0 = sem follow de links
REQUEST_DELAY = 0.8                 # segundos entre requisições

# Categoria padrão
DEFAULT_CATEGORY = "generic"

# CATEGORIAS + palavras-chave
CATEGORIES = [
    ("streaming", "Streaming", ["video", "movie", "series", "stream", "netflix", "youtube", "spotify", "radio", "mp3", "podcast", "flv", "mkv", "mp4", "dvd", "audio", "music", "filme", "série", "canal", "play"], "video|movie|stream|audio|music|series|mp3|podcast|radio|flv|mkv|mp4", "either"),
    ("blog", "Blog/Artigo", ["blog", "article", "post", "news", "journal", "diary", "writing", "artigo", "post", "notícia", "newsletter"], "blog|article|post|news|jornal|diário|escrit|publicação", "either"),
    ("chat", "Chat/Mensageiro", ["chat", "talk", "forum", "community", "message", "messenger", "discord", "telegram", "conversa", "fórum", "grupo"], "chat|talk|forum|message|discord|telegram|conversa|fórum", "either"),
    ("wiki", "Wiki/Enciclopédia", ["wiki", "encyclopedia", "knowledge", "reference", "class", "glossary", "enciclopédia", "wikipedia"], "wiki|encyclopedia|knowledge|reference|class", "either"),
    ("marketplace", "Loja/Compras", ["market", "shop", "store", "buy", "sell", "cart", "marketplace", "loja", "compre", "venda", "carrinho"], "market|shop|buy|sell|cart|loja|compre|venda", "either"),
    ("search", "Busca", ["search", "engine", "find", "portal", "index", "busca", "motor", "página"], "search|engine|find|portal|busca", "either"),
    ("news", "Notícias/Jornal", ["news", "press", "report", "coverage", "jornal", "notícia", "revista", "paper"], "news|press|report|revista|paper|jornal", "either"),
    ("hacking", "Hacking/Cracking", ["hack", "crack", "phishing", "keylogger", "exploit", "vulnerability", "hack", "cracking", "hacking", "exploração"], "hack|crack|phishing|exploit|vulnerability|hacking", "either"),
    ("books", "Livros", ["book", "reading", "library", "novel", "poem", "poetry", "livro", "leitura", "biblioteca", "romance", "poesia"], "book|reading|library|novel|poem|poetry|livro|leitura|biblioteca|romance|poesia", "either"),
    ("finance", "Finanças/Pagamento", ["crypto", "bitcoin", "finance", "payment", "wallet", "exchange", "cripto", "dinheiro", "pagamento", "cripto", "moeda", "wallet"], "crypto|bitcoin|finance|payment|wallet|exchange|cripto|dinheiro|pagamento|moeda", "either"),
]

ROUTING_WORDS = ["bot", "crawler", "index", "directory", "archive", "list", "portal", "search", "engine"]

# ── Armazenamento de persistência (snapshot de state) ──────────────────────
STATE_FILE = os.path.join(OUTPUT_DIR, "visited_urls.json")

def load_visited(quiet=False):
    """Carrega o set de URLs já visitadas de um arquivo."""
    global VISITED_URLS
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                VISITED_URLS = set(data)
            if not quiet:
                log(f"State carregado: {len(VISITED_URLS)} URLs", quiet)
        except Exception as e:
            log_error(f"Erro ao carregar state: {e}")
            VISITED_URLS = set()
    else:
        VISITED_URLS = set()


def save_visited(quiet=False):
    """Salva o set de URLs já visitadas em um arquivo."""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(list(VISITED_URLS), f, ensure_ascii=False)
        if not quiet:
            log(f"State salvo: {len(VISITED_URLS)} URLs", quiet)
    except Exception as e:
        log_error(f"Erro ao salvar state: {e}")


# ── Cliente HTTP via Trobot (httpx SOCKS5) ─────────────────────────────────
http_client = httpx.Client(
    proxy=TOR_SOCKS_PROXY,
    timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT),
    follow_redirects=True,
    headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    },
    verify=False,
)


def log(msg, quiet=False):
    if not quiet:
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def log_error(msg):
    print(f"[ERRO] {msg}", flush=True)


def is_onion_url(url):
    try:
        hostname = url.split("://")[1].split("/")[0]
        return hostname.endswith(".onion")
    except Exception:
        return False


def extract_links(html_bytes):
    """Extrai URLs de <a href>. Retorna lista de URLs .onion (deduplicadas)."""
    try:
        soup = BeautifulSoup(html_bytes, "html.parser")
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("http://") or href.startswith("https://"):
                if is_onion_url(href):
                    links.add(href)
            elif href.startswith("#"):
                continue
        return list(links)
    except Exception:
        return []


def extract_metadata(html_bytes):
    """Extrai título e descrição. Retorna dict."""
    title = ""
    description = ""
    try:
        soup = BeautifulSoup(html_bytes, "html.parser")
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            description = meta_desc["content"].strip()
    except Exception:
        pass
    return {"title": title[:300], "description": description[:500]}


def detect_language(text):
    """Detecta idioma (pt / en / generic)."""
    if not text:
        return "generic"
    lower = text.lower()
    pt_words = ["o ", "a ", "de ", "do ", "da ", "que ", "para ", "com ", "como ", "por ",
                "mais ", "mas ", "ser ", "tem ", "sem ", "sua ", "nossa ", "seu ", "é ",
                "há ", "você ", "vocês ", "eles ", "elas ", "este ", "essa ", "aquele ",
                "aquela ", "aqui ", "ontem ", "hoje ", "amanhã ", "semanas ", "meses ",
                "anos ", "cidade ", "país ", "nacional ", "internacional "]
    en_words = [" the ", "and ", "is ", "in ", "to ", "of ", "it ", "you ", "that ", "he ",
                "was ", "for ", "on ", "are ", "with ", "as ", "i ", "his ", "they ", "be ",
                "at ", "one ", "have ", "this ", "from ", "or ", "had ", "by ", "not ", "what ",
                "all ", "were ", "we ", "when ", "your ", "can ", "said ", "there ", "use ",
                "an ", "each ", "which ", "she ", "do ", "how ", "their ", "if ", "will ",
                "up ", "other ", "about ", "out ", "many ", "then ", "them ", "these ",
                "so ", "some ", "her ", "would ", "make ", "like ", "him ", "into ", "time ",
                "has ", "look ", "two ", "more ", "write ", "go ", "see ", "number ", "no ",
                "way ", "could ", "people ", "my ", "than ", "first ", "water ", "been ",
                "call ", "who ", "oil ", "its ", "now ", "find ", "long ", "down ", "day ",
                "did ", "get ", "come ", "made ", "may ", "part "]
    pt_count = sum(1 for w in pt_words if w in lower)
    en_count = sum(1 for w in en_words if w in lower)
    has_accent = bool(re.search(r"[ãàâáéêíóôõõúü]", lower))
    if has_accent:
        pt_count += 3
    if pt_count >= 4 and pt_count > en_count * 2:
        return "pt"
    if en_count >= 4 and en_count > pt_count * 2:
        return "en"
    if has_accent:
        return "pt"
    return "generic"


def categorize(title, description):
    text = (title + " " + description).lower()
    if not text:
        return DEFAULT_CATEGORY
    for key, name, left_kw, right_kw, mode in CATEGORIES:
        left_list = left_kw if isinstance(left_kw, list) else [left_kw]
        right_list = right_kw.split("|") if isinstance(right_kw, str) else right_kw
        left_hit = any(kw in text for kw in left_list)
        right_hit = any(kw in text for kw in right_list)
        if mode == "either" and (left_hit or right_hit):
            return key
        if mode == "both" and left_hit and right_hit:
            return key
        if mode == "left" and left_hit:
            return key
        if mode == "right" and right_hit:
            return key
    return DEFAULT_CATEGORY


def is_routing_site(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in ROUTING_WORDS)


def fetch_page(domain):
    """
    Busca uma página via Trobot (httpx SOCKS5) e extrai metadados.
    Também extrai links internos e segue até MAX_CHILDREN.
    Retorna dict com resultados.
    """
    url = domain
    result = {
        "domain": domain,
        "url": url,
        "status": "error",
        "title": "",
        "description": "",
        "category": DEFAULT_CATEGORY,
        "language": "generic",
        "error": "",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "via_proxy": "tor",
        "children_found": 0,
        "children_followed": 0,
    }
    try:
        resp = http_client.get(url, follow_redirects=True)
        body = resp.content
        if len(body) > 50:
            meta = extract_metadata(body)
            result["title"] = meta["title"]
            result["description"] = meta["description"]
            lang_text = (meta["title"] + " " + meta["description"]).strip()
            if not lang_text:
                lang_text = body.decode("utf-8", errors="ignore")[:2000]
            result["language"] = detect_language(lang_text)
            result["category"] = categorize(meta["title"], meta["description"])
            if is_routing_site(meta["title"], meta["description"]):
                if result["category"] == DEFAULT_CATEGORY:
                    result["category"] = "generic"
            result["status"] = "success"

            # Extrair e seguir links internos
            links = extract_links(body)
            onion_links = [l for l in links if is_onion_url(l)]
            result["children_found"] = len(onion_links)

            follow_count = 0
            for child in onion_links:
                if follow_count >= MAX_CHILDREN_PER_SITE:
                    break
                normalized = child.split("?")[0].split("#")[0].rstrip("/") + "/"
                if normalized not in VISITED_URLS:
                    VISITED_URLS.add(normalized)
                    result["children_followed"] += 1
                    follow_count += 1
    except Exception as e:
        result["error"] = str(e)
    return result


def scrape_all_domains(domains, quiet=False, fast=False, limit=None, max_children=50):
    """Scrape todos os domínios. Cada domínio pode gerar até `max_children` links filhos."""
    global MAX_CHILDREN_PER_SITE
    MAX_CHILDREN_PER_SITE = max_children

    if fast:
        # Filtra domínios já visitados (usando hashmap carregado)
        domains = [d for d in domains if d not in VISITED_URLS]
        if not quiet:
            log(f"Já visitados: {len(domains)} | Novos: {len(domains)}", quiet)

    if not quiet:
        log(f"Novo total a processar: {len(domains)}", quiet)
        log(f"Threads ativadas: {THREADS} | Timeout: {TIMEOUT}s | Max children/site: {max_children}", quiet)
        log(f"Total de URLs já visitadas (deduplicação): {len(VISITED_URLS)}", quiet)

    if limit:
        domains = domains[:limit]

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_domain = {executor.submit(fetch_page, d): d for d in domains}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                result = future.result()
                results.append(result)
                if result["status"] == "success":
                    completed += 1
                    if not quiet:
                        log(f"  ✓ {domain} -> {result['category']} [{result['language']}] "
                            f"(children found: {result.get('children_found', 0)}, followed: {result.get('children_followed', 0)})", quiet)
                else:
                    log(f"  ✗ {domain} -> {result.get('error', 'unknown')}", quiet)
            except Exception as e:
                result = {"domain": domain, "status": "error", "error": str(e), "via_proxy": "tor"}
                results.append(result)
                log(f"  ✗ {domain} -> exception: {e}", quiet)

            time.sleep(REQUEST_DELAY)
            completed += 1
            if limit and completed >= limit:
                break

    return results


def save_results(results, save_dir):
    """Salva resultados em CSV e JSON."""
    os.makedirs(save_dir, exist_ok=True)

    fieldnames = [
        "domain", "url", "status", "title", "description", "category",
        "language", "error", "fetched_at", "via_proxy",
        "children_found", "children_followed"
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    json_data = [{k: r.get(k, "") for k in fieldnames} for r in results]
    with open(META_JSON, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    return CSV_PATH, META_JSON


def print_report(results):
    total = len(results)
    success = sum(1 for r in results if r["status"] == "success")
    categories = {}
    languages = {}
    for r in results:
        if r["status"] == "success":
            cat = r.get("category", "generic")
            lang = r.get("language", "generic")
            categories[cat] = categories.get(cat, 0) + 1
            languages[lang] = languages.get(lang, 0) + 1
    print(f"\n=== Relatório de Crawl ===")
    print(f"Total: {total} | Sucesso: {success}")
    print(f"Categorias: {dict(categories)}")
    print(f"Idiomas: {dict(languages)}")
    errors = [r for r in results if r["status"] == "error"]
    if errors:
        print(f"\nErros detalhados ({len(errors)} errors):")
        for r in errors[:10]:
            print(f"  - {r.get('domain')}: {r.get('error', 'n/a')}")


def main():
    parser = argparse.ArgumentParser(
        description="Crawler de sites .onion via Ahmia + Trobot (link follow + dedup).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--fast", action="store_true", help="Pular URLs já visitadas (hashmap)")
    parser.add_argument("--quiet", action="store_true", help="Reduz logs")
    parser.add_argument("--limit", type=int, default=None, help="Limite de domínios")
    parser.add_argument("--threads", type=int, default=4, help="Número de threads")
    parser.add_argument("--max-children", type=int, default=50, help="Máximo de links filhos por site")
    args = parser.parse_args()

    global THREADS
    THREADS = args.threads

    # Carregar estado persistido (resumo de onde parou)
    load_visited(quiet=args.quiet)

    # 1. Verificar conexão via Tor
    log("Verificando conexão via Tor...", quiet=args.quiet)
    try:
        resp = http_client.get("https://check.torproject.org/api/ip")
        ip_match = re.search(r'"IP":\s*"([^"]+)"', resp.text)
        if ip_match:
            print(f"Tor proxy confirmado: exit node {ip_match.group(1)}")
        else:
            print("Tor proxy ativo (sem IP detectável)")
    except Exception as e:
        log_error(f"Erro de conexão com Tor: {e}")
        sys.exit(1)

    # 2. Baixar índice Ahmia
    log("Baixando índice Ahmia...", quiet=args.quiet)
    try:
        resp = http_client.get(AHMI_INDEX_URL)
        html = resp.content.decode("utf-8", errors="ignore")
    except Exception as e:
        log_error(f"Falha ao baixar índice: {e}")
        sys.exit(1)

    pattern = re.compile(r'http://([a-z22222468]{56})\.onion/')
    domains = pattern.findall(html)
    domains = [f"http://{d}.onion/" for d in domains]
    seen = set()
    unique_domains = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            unique_domains.append(d)
    domain_count = len(unique_domains)
    log(f"Encontrados {domain_count} domínios únicos no índice Ahmia", quiet=args.quiet)

    # 3. Processar domínios (BFS com dedup)
    log("Iniciando scrape com Trobot + threading...", quiet=args.quiet)
    results = scrape_all_domains(
        unique_domains,
        quiet=args.quiet,
        fast=args.fast,
        limit=args.limit,
        max_children=args.max_children
    )

    # 4. Relatório
    print_report(results)

    # 5. Salvar resultados
    csv_path, json_path = save_results(results, OUTPUT_DIR)
    log(f"Resultados salvos em:\n  - {csv_path}\n  - {json_path}", quiet=args.quiet)

    # 6. Resumo final
    print_report(results)

    # Salvar estado persistido para continuidade
    save_visited(quiet=args.quiet)

if __name__ == "__main__":
    main()
