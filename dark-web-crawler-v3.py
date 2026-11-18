#!/usr/bin/env python3
"""
dark-web-crawler-v3.py — Crawler de sites .onion via Ahmia + Trobot (httpx).

Novidades:
  1. Usa módulos do Trobot (/home/ubuntu/tools/TorBot/) diretamente
     para crawl (httpx + socksio) e armazenamento de resultados.
  2. Armazena achados diferenciando por categoria.
  3. Leitura de CSV/JSON dentro do script (não script separado).
  4. Validação de linguagem unificada (detect_language()).
  5. Armazenamento com hashmap (VISITED_URLS) para evitar duplicados.

Uso:
    /home/ubuntu/tools/TorBot/.venv/bin/python dark-web-crawler-v3.py              # modo interativo
    --fast            # só novos (pula URLs já na CSV)
    --quiet           # sem logs interativos
    --threads N       # batch de N threads
    --max-children N  # máx. links filhos por site (0 = sem follow)
    --limit N         # limite de domínios iniciais

Dependências:
    /home/ubuntu/tools/TorBot/.venv/bin/python dark-web-crawler-v3.py
    (httpx 0.27, beautifulsoup4, socksio, ijson)
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
    import ijson
    from httpx import Timeout
except ImportError as e:
    print(f"[ERRO] Dependência faltando: {e}", flush=True)
    sys.exit(1)

# ── Configuração global ─────────────────────────────────────────────────────
TOR_SOCKS_PROXY = "socks5://127.0.0.1:9050"   # httpx 0.27 só aceita socks5://
AHMI_INDEX_URL = "https://ahmia.fi/onions/"
OUTPUT_DIR = os.path.expanduser("~/torbot-catalog")
CSV_PATH = os.path.join(OUTPUT_DIR, "catalog.csv")
META_JSON = os.path.join(OUTPUT_DIR, "results.json")
STATE_JSON = os.path.join(OUTPUT_DIR, "visited_urls.json")
TIMEOUT = 15
THREADS = 4
MAX_CHILDREN_PER_SITE = 50          # 0 = sem follow de links
REQUEST_DELAY = 0.8                 # segundos entre requisições

# Categoria padrão
DEFAULT_CATEGORY = "generic"

# CATEGORIAS + palavras-chave
CATEGORIES = [
    # (key, display, left_keywords, right_keywords, mode)
    ("streaming", "Streaming", ["stream", "video", "player", "yt", "plex", "jellyfin", "emby"], "rtmp|http|https", "either"),
    ("blog", "Blog", ["blog", "post", "article", "news", "page"], "post|article|news|write", "either"),
    ("chat", "Chat", ["chat", "telegram", "discord", "forum", "talk"], "chat|forum|talk", "either"),
    ("wiki", "Wiki", ["wiki", "encyclopedia", "knowledge", "articles"], "wiki|encyclopedia", "either"),
    ("marketplace", "Marketplace", ["market", "shop", "store", "cart", "buy", "sell", "payment", "crypto", "bitcoin"], "shop|cart|payment|buy|market", "either"),
    ("search", "Search", ["search", "engine", "query", "search", "find", "lookup"], "search|engine|query|find", "either"),
    ("finance", "Finance", ["finance", "crypto", "wallet", "exchange", "market", "trading"], "finance|wallet|exchange|trading|market", "either"),
    ("personals", "Pessoais", ["personals", "dating", "contact", "relationship"], "dating|contact|relationship", "either"),
    ("hacking", "Hacking", ["hacking", "exploit", "crack", "tool", "urlish"], "hacking|exploit|crack|tool", "either"),
    ("crypto", "Cripto", ["crypto", "bitcoin", "monero", "coin", "wallet"], "crypto|bitcoin|monero", "either"),
    ("services", "Serviços", ["service", "support", "help", "hosting", "domain", "vpn", "vault"], "hosting|service|support|vpn", "either"),
    ("email", "Email", ["email", "mail", "sms", "leak"], "email|mail|sms|leak", "either"),
]

# Palavras para detectar se é um site de roteamento (navegação)
ROUTING_WORDS = ["index", "home", "login", "register", "sign", "status", "about", "contact", "help", "support", "forum", "community"]

# HTTP client com proxy SOCKS5
http_client = httpx.Client(
    proxy=TOR_SOCKS_PROXY,
    timeout=Timeout(TIMEOUT, read=TIMEOUT, write=TIMEOUT, connect=TIMEOUT),
    follow_redirects=True,
    verify=False,
)


# ── Validação de linguagem unificada ────────────────────────────────────────
def detect_language(text):
    """Detecta idioma (pt / en / generic). UNIFICADO — usado em todos os lugares."""
    if not text:
        return "generic"
    lower = text.lower()
    pt_words = ["o ", "a ", "de ", "do ", "da ", "que ", "para ", "com ", "como ", "por ",
                "mais ", "mas ", "ser ", "tem ", "sem ", "sua ", "nossa ", "seu ", "é ",
                "há ", "você ", "vocês ", "eles ", "elas ", "este ", "essa ", "aquele ",
                "aquela ", "aqui ", "ontem ", "hoje ", "amanhã ", "semanas ", "meses ",
                "anos ", "cidade ", "país ", "nacional ", "internacional "]
    en_words = [" the ", " and ", " is ", " in ", " to ", " of ", " it ", " you ", " that ",
                " he ", " was ", " for ", " on ", " are ", " with ", " as ", " i ", " his ",
                " they ", " be ", " at ", " one ", " have ", " this ", " from ", " or ",
                " had ", " by ", " not ", " what ", " all ", " were ", " we ", " when ",
                " your ", " can ", " said ", " there ", " use ", " an ", " each ", " which ",
                " she ", " do ", " how ", " their ", " if ", " will ", " up ", " other ",
                " about ", " out ", " many ", " then ", " them ", " these ", " so ", " some ",
                " her ", " would ", " make ", " like ", " him ", " into ", " time ", " has ",
                " look ", " two ", " more ", " write ", " go ", " see ", " number ", " no ",
                " way ", " could ", " people ", " my ", " than ", " first ", " water ",
                " been ", " call ", " who ", " oil ", " its ", " now ", " find ", " long ",
                " down ", " day ", " did ", " get ", " come ", " made ", " may ", " part "]
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


# ── Categorização ───────────────────────────────────────────────────────────
def categorize(title, description):
    """Categoriza o site baseado em palavras-chave. Heurística (0% IA)."""
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
    """Verifica se é um site de roteamento/navegação."""
    text = (title + " " + description).lower()
    return any(w in text for w in ROUTING_WORDS)


# ── Extração de metadados ───────────────────────────────────────────────────
def extract_metadata(body):
    """Extrai <title> e <meta name="description"> do HTML."""
    soup = BeautifulSoup(body, "html.parser")
    title = soup.find("title")
    title = title.get_text(strip=True) if title else ""
    meta = soup.find("meta", attrs={"name": "description"})
    description = meta.get("content", "").strip() if meta else ""
    return {"title": title[:300], "description": description[:500]}


def extract_links(body):
    """Extrai todos os links <a href> do HTML."""
    soup = BeautifulSoup(body, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href:
            links.append(href)
    return links


def is_onion_url(url):
    """Verifica se é um domínio .onion."""
    return bool(re.search(r"\.onion\b", url))


# ── Trobot integration core ─────────────────────────────────────────────────
# Usa os módulos do Trobot para o trabalho pesado de crawling

def fetch_page_with_torbot(domain):
    """
    Busca uma página usando o Trobot (LinkTree + fetch_html).
    Usa a estrutura do projeto local /home/ubuntu/tools/TorBot/.
    """
    result = {
        "domain": domain,
        "url": domain,
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

    # Configurar o cliente SOCKS5 (httpx 0.27 apenas suporta socks5://)
    client = httpx.Client(
        proxy=TOR_SOCKS_PROXY,
        timeout=Timeout(TIMEOUT, read=TIMEOUT, write=TIMEOUT, connect=TIMEOUT),
        follow_redirects=True,
        verify=False,
    )

    try:
        # Usar o LinkTree do Trobot para crawler
        tree = LinkTree(root=domain, client=client, depth=1)
        tree.crawl()

        # Extrair resultados do LinkTree
        for node in tree.all_nodes_itr():
            title = node.data.title if hasattr(node.data, 'title') and node.data.title else ""
            description = ""
            if hasattr(node.data, 'description') and node.data.description:
                description = node.data.description

            if title:
                result["title"] = title[:300]
                result["description"] = description[:500]

                # Unificar linguagem
                lang_text = (title + " " + description).strip()
                if not lang_text:
                    lang_text = node.raw_content[:2000] if hasattr(node, 'raw_content') else body.decode('utf-8', errors='ignore')[:2000]
                result["language"] = detect_language(lang_text)

                # Categorizar
                result["category"] = categorize(title, description)

                if is_routing_site(title, description):
                    if result["category"] == DEFAULT_CATEGORY:
                        result["category"] = "generic"

                result["status"] = "success"

                # Extrair links internos
                links = tree.all_links()
                onion_links = [l for l in links if is_onion_url(l)]
                result["children_found"] = len(onion_links)

                # Seguir links filhos (hashmap dedup)
                for child in onion_links:
                    normalized = child.split("?")[0].split("#")[0].rstrip("/") + "/"
                    if normalized not in VISITED_URLS:
                        VISITED_URLS.add(normalized)
                        result["children_followed"] += 1

                break  # Só processar o primeiro nó

    except Exception as e:
        result["error"] = str(e)
    finally:
        client.close()

    return result


# ── Estado / persistence ────────────────────────────────────────────────────
VISITED_URLS = set()
STATE_FILE = STATE_JSON


def load_visited(quiet=False):
    """Carrega estado persistido de URLs já visitadas."""
    global VISITED_URLS
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                VISITED_URLS = set(json.load(f))
            if not quiet:
                print(f"[INFO] URLs já visitadas: {len(VISITED_URLS)}", flush=True)
        except Exception as e:
            print(f"[ERRO] Não foi possível carregar estado: {e}", flush=True)
            VISITED_URLS = set()


def save_visited(quiet=False):
    """Salva estado persistido de URLs visitadas."""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(list(VISITED_URLS), f, ensure_ascii=False)
        if not quiet:
            print(f"[INFO] Estado salvo: {len(VISITED_URLS)} URLs", flush=True)
    except Exception as e:
        print(f"[ERRO] Não foi possível salvar estado: {e}", flush=True)


# ── Crawl principal ────────────────────────────────────────────────────────
def scrape_all_domains(domains, quiet=False, fast=False, limit=None, max_children=50):
    """Scrape todos os domínios. Cada domínio pode gerar até `max_children` links filhos."""
    global MAX_CHILDREN_PER_SITE, VISITED_URLS
    MAX_CHILDREN_PER_SITE = max_children

    if fast:
        domains = [d for d in domains if d not in VISITED_URLS]
        if not quiet:
            print(f"[INFO] Já visitados: {len(domains)} | Novos: {len(domains)}", flush=True)

    if not quiet:
        print(f"[INFO] Novo total a processar: {len(domains)}", flush=True)
        print(f"[INFO] Threads ativadas: {THREADS} | Timeout: {TIMEOUT}s | Max children/site: {max_children}", flush=True)
        print(f"[INFO] Total de URLs já visitadas (deduplicação): {len(VISITED_URLS)}", flush=True)

    if limit:
        domains = domains[:limit]

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_domain = {executor.submit(fetch_page_with_torbot, d): d for d in domains}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                result = future.result()
                results.append(result)
                if result["status"] == "success":
                    completed += 1
                    if not quiet:
                        print(f"  ✓ {domain} -> {result['category']} [{result['language']}] "
                              f"(children found: {result.get('children_found', 0)}, followed: {result.get('children_followed', 0)})", flush=True)
                else:
                    if not quiet:
                        print(f"  ✗ {domain} -> {result.get('error', 'unknown')}", flush=True)
            except Exception as e:
                result = {"domain": domain, "status": "error", "error": str(e), "via_proxy": "tor"}
                results.append(result)
                if not quiet:
                    print(f"  ✗ {domain} -> exception: {e}", flush=True)
            time.sleep(REQUEST_DELAY)
            completed += 1
            if limit and completed >= limit:
                break

    return results


# ── Leitura de CSV/JSON ─────────────────────────────────────────────────────
def read_catalog_csv(path=None):
    """Lê o catalog.csv e retorna lista de domínios."""
    path = path or CSV_PATH
    domains = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                domains.append(row.get("domain", ""))
    return domains


def read_results_json(path=None):
    """Lê o results.json e retorna registros."""
    path = path or META_JSON
    records = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)
    return records


def print_report(results):
    """Imprime relatório de resumos."""
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
    print(f"Total: {total} | Sucesso: {success} | Erros: {total - success}")
    print(f"Categorias: {dict(sorted(categories.items(), key=lambda x: -x[1]))}")
    print(f"Idiomas: {dict(sorted(languages.items(), key=lambda x: -x[1]))}")
    errors = [r for r in results if r["status"] == "error"]
    if errors:
        print(f"\nErros detalhados ({len(errors)} erros):")
        for r in errors[:10]:
            print(f"  - {r.get('domain')}: {r.get('error', 'n/a')}")


# ── Main ────────────────────────────────────────────────────────────────────
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

    global THREADS, STATE_FILE, CSV_PATH, META_JSON
    THREADS = args.threads
    STATE_FILE = os.path.join(OUTPUT_DIR, "visited_urls.json")

    # Carregar estado persistido
    load_visited(quiet=args.quiet)

    # 1. Verificar conexão via Tor
    print("[INFO] Verificando conexão via Tor...", flush=True)
    try:
        resp = http_client.get("https://check.torproject.org/api/ip")
        ip_match = re.search(r'"IP":\s*"([^"]+)"', resp.text)
        if ip_match:
            print(f"[OK] Tor proxy confirmado: exit node {ip_match.group(1)}", flush=True)
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar via Tor: {e}", flush=True)
        sys.exit(1)

    # 2. Baixar índice Ahmia
    print("[INFO] Baixando índice Ahmia...", flush=True)
    try:
        resp = http_client.get(AHMI_INDEX_URL)
        resp.raise_for_status()
        raw = resp.text
        # Extrair domínios .onion do HTML
        domains = re.findall(r'([a-z2-7]{56}\.onion)', raw)
        # Remover duplicados (manter ordem)
        seen = set()
        unique_domains = []
        for d in domains:
            if d not in seen:
                seen.add(d)
                unique_domains.append(d)
        print(f"[OK] {len(unique_domains)} domínios .onion únicos encontrados", flush=True)
    except Exception as e:
        print(f"[ERRO] Não foi possível baixar o índice Ahmia: {e}", flush=True)
        sys.exit(1)

    # 3. Scraping
    print(f"[INFO] Iniciando crawl com {THREADS} threads...", flush=True)
    results = scrape_all_domains(
        unique_domains,
        quiet=args.quiet,
        fast=args.fast,
        limit=args.limit,
        max_children=args.max_children
    )

    # 4. Salvar resultados
    print("[INFO] Salvando resultados...", flush=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
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

    save_visited()

    # 5. Relatório
    print_report(results)

    # 6. Resumo final
    print(f"\n[OK] Crawl concluído: {len(results)} domínios processados", flush=True)
    print(f"[OK] Continuidade garantida via {STATE_FILE}", flush=True)
    print(f"[OK] Salvo: {CSV_PATH}", flush=True)
    print(f"[OK] Salvos: {META_JSON}", flush=True)


if __name__ == "__main__":
    main()
