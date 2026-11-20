#!/usr/bin/env python3
"""
dark-web-crawler-v3.py — Crawler de sites .onion via Ahmia index + Trobot
(uso das funcionalidades do Trobot, sem reescrever o crawler).

Premissas aplicadas:
  1. Usa o Trobot (LinkTree + CrawlResultAdapter) para a árvore de links e
     o classify/categorize do Trobot sem reescrever o crawler torbot.
  2. Fetch via túnel SOCKS5 curl-equivalent (handshake + auth + CONNECT com DNS
     remoto), igual ao `curl --socks5-hostname 127.0.0.1:9050`. Isso corrige o
     problema que o httpx 0.27 (proxy socks5://) não resolve .onion e não faz o
     auth SOCKS5 que o Tor exige.
  3. Idioma: pt / en (heuristicas puros, 0% IA), igual ao v3 original.
  4. Categorias simples: marketplace, wiki, fórum (chat), streaming, generic.
  5. Hidden wikis: Deep-2 — segue children e filhos de filhos consecutivamente
     (LinkTree depth=2) para mapear a árvore.
  6. Sem threads no modo incremental (seqüencial, 1 domínio por vez).
  7. Incremental: salva progresso em visited_urls.json + catalog.csv.
  8. Depois dos 10 domínios, testa concorrência com ThreadPoolExecutor.

Dependências do Trobot (jit): src/torbot/modules/linktree.py (LinkTree),
src/torbot/crawl_result.py (CrawlResultAdapter).
"""

from urllib.parse import urlsplit

import argparse
import csv
import json
import os
import re
import socket
import ssl
import struct
import subprocess
import sys
import time
import http.client
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Trobot / dependências ─────────────────────────────────────────────────────
TORBOT_SRC = os.path.expanduser("/home/ubuntu/torbot-catalog/tools/TorBot/src")
if TORBOT_SRC not in sys.path:
    sys.path.insert(0, TORBOT_SRC)

try:
    import httpx
    from bs4 import BeautifulSoup

    from torbot.crawl_result import CrawlResultAdapter
    from torbot.modules.info import fetch_html, display_webpage_description
    from torbot.modules.linktree import LinkTree, parse_links
    from torbot.modules.nlp.main import extract_text as nlp_extract_text
except ImportError as e:  # pragma: no cover
    print(f"[ERRO] Dependência faltando: {e}", flush=True)
    sys.exit(1)

import numpy as np

TORBOT_SRC_DIR = os.path.dirname(TORBOT_SRC)
try:
    from torbot.analyst import build_evidence, build_graph, deterministic_claims
    ANALYST_AVAILABLE = True
except ImportError:
    ANALYST_AVAILABLE = False

try:
    from torbot.modules.color import color
    from torbot.modules.updater import check_version
    FROM_COLOR = True
except Exception:
    FROM_COLOR = False

OUT_DIR = "/home/ubuntu/torbot-catalog"
CSV_PATH = os.path.join(OUT_DIR, "catalog.csv")
META_JSON = os.path.join(OUT_DIR, "results.json")
STATE_JSON = os.path.join(OUT_DIR, "visited_urls.json")
ANALYSIS_JSON = os.path.join(OUT_DIR, "analysis_report.json")
TIMEOUT = 15
REQUEST_DELAY = 0.8
DEFAULT_CATEGORY = "generic"
AHMI_CACHE_PATH = os.path.expanduser("~/ahmia_index.html")
TOR_SOCKS = "127.0.0.1:9050"

# ── Categorias simples (5) ───────────────────────────────────────────────────
CATEGORIES = [
    ("streaming", "Streaming", ["stream", "video", "player", "yt", "plex", "jellyfin", "emby"], ["rtmp|http|https"]),
    ("wiki", "Wiki", ["wiki", "encyclopedia", "knowledge", "articles"], ["wiki|encyclopedia"]),
    ("chat", "Fórum / Chat", ["forum", "chat", "talk", "discuss", "board"], ["forum|talk|chat"]),
    ("marketplace", "Marketplace", ["market", "shop", "store", "cart", "buy", "sell", "payment", "crypto", "bitcoin"], ["shop|cart|market|buy|payment|wallet"]),
    ("generic", "Genérico", [], []),
]

ROUTE_WORDS = ["index", "home", "login", "register", "sign", "status", "about", "contact", "help", "support"]


# ── Fetch via túnel SOCKS5 (coração da correção) ─────────────────────────────
def _socks5_connect(host, port=443, timeout=15):
    """Conecta ao Tor SOCKS5 como o curl --socks5-hostname.

    Handshake: [5,2,0,2] -> [5,2] (servidor seleciona auth) -> [5,2,0,0,0,0]
    (senha vazia) -> CONNECT com hostname (DNS remoto do Tor).
    """
    s = socket.create_connection(("127.0.0.1", 9050), timeout=timeout)
    s.settimeout(timeout)
    try:
        s.sendall(bytes([5, 2, 0, 2]))
        s.recv(2)
        s.sendall(bytes([5, 2, 0, 0, 0, 0]))
        s.recv(4)
        dom = host.encode()
        req = bytes([5, 1, 0, 3]) + bytes([len(dom)]) + dom + struct.pack(">H", port)
        s.sendall(req)
        resp = s.recv(4)
        atyp = resp[3]
        if atyp == 3:
            ln = s.recv(1)[0]
            s.recv(ln)
        elif atyp == 1:
            s.recv(6)
        elif atyp == 4:
            s.recv(18)
        s.recv(2)  # porta do endereço ligado
        if resp[1] != 0:
            raise ConnectionError("proxy refused: %d" % resp[1])
        return s
    except Exception:
        s.close()
        raise


def tor_fetch(url, retries=3):
    """Obtém o HTML de um .onion/URL via Tor, igual ao curl --socks5-hostname.

    Retorna ``(status, html_bytes)``. Em caso de falha (Tor instável),
    recua usando a execução de `curl` via subprocess — o que garante que o
    script segue funcionando assim como o curl.
    """
    if "://" not in url:
        url = "http://" + url
    parsed = urlsplit(url)
    host = parsed.hostname or url
    port = parsed.port or 443
    last_err = None
    for attempt in range(retries):
        try:
            s = _socks5_connect(host, port)
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ss = ctx.wrap_socket(s, server_hostname=host)
            conn = http.client.HTTPSConnection(host, port=port, timeout=TIMEOUT, context=ctx)
            conn.sock = ss
            conn.request("GET", "/", headers={"User-Agent": "TorbotCatalog/3.0"})
            resp = conn.getresponse()
            data = resp.read()
            ss.close()
            s.close()
            return resp.status, data
        except Exception as e:
            last_err = e
            time.sleep(REQUEST_DELAY * (attempt + 1))
    # fallback provado: executa o curl equivalente
    cmd = ["curl", "--socks5-hostname", "127.0.0.1:9050", "-L", "--max-time", str(TIMEOUT), url]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=TIMEOUT + 5)
        return (r.returncode, r.stdout)
    except Exception as e2:
        return (-1, ("fallback-failed: %s; raw: %s" % (e2, last_err)).encode())


def parse_title_description(html_bytes):
    """Extrai título + descrição semelhante ao LinkTree do Trobot."""
    text = html_bytes.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    title = (soup.title.text.strip() if soup.title else "")
    body = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    return title, body[:1000]


# ── Idioma (heurística, 0% IA) ───────────────────────────────────────────────
def detect_language(text):
    if not text:
        return "generic"
    lower = text.lower()
    pt_words = ["o ", "a ", "de ", "do ", "da ", "que ", "para ", "com ", "como ", "por ",
                "mais ", "mas ", "ser ", "tem ", "sem ", "sua ", "nossa ", "seu ", "é ",
                "há ", "você ", "eles ", "elas ", "este ", "essa ", "aquele ", "aquela ",
                "aqui ", "ontem ", "hoje ", "amanhã ", "semanas ", "meses ", "anos ",
                "cidade ", "país ", "nacional ", "internacional "]
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


# ── Categoria + roteamento (reutiliza a lógica do Trobot) ────────────────────
def _categorize(title, description):
    text = (title + " " + description).lower()
    if not text:
        return DEFAULT_CATEGORY
    for key, name, left_kw, right_kw in CATEGORIES:
        left_list = left_kw if isinstance(left_kw, list) else [left_kw]
        right_list = right_kw.split("|") if isinstance(right_kw, str) else right_kw
        left_hit = any(kw in text for kw in left_list)
        right_hit = any(kw in text for kw in right_list)
        if key == "generic":
            continue
        if (left_hit or right_hit):
            return key
    return DEFAULT_CATEGORY


def is_routing_site(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in ROUTE_WORDS)


# ── Trobot LinkTree (árvore de filhos do hidden wiki) ────────────────────────
def follow_wiki_children(domain, depth=2, max_children=10):
    """Usa o LinkTree do Trobot para percorrer children e filhos(de filhos) de
    um hidden wiki, imitando o comportamento 'children and grandchildren'
    consecutivamente."""
    results = []
    try:
        tree = LinkTree(url=domain, depth=depth, client=None)
        tree.load()
        for node in tree.all_nodes_itr():
            results.append((node.identifier, node.data.title if node.data else ""))
    except Exception as e:
        if not FROM_COLOR:
            print(f"[WARN] Trobot LinkTree falhou em {domain}: {e}", flush=True)
        results = [(domain, "")]
    return results


# ── Estado incremental ──────────────────────────────────────────────────────
VISITED = set()


def load_visited(quiet=False):
    global VISITED
    if os.path.exists(STATE_JSON):
        try:
            VISITED = set(json.load(open(STATE_JSON, encoding="utf-8")))
            if not quiet:
                print(f"[INFO] Já visitados: {len(VISITED)} URLs.", flush=True)
        except Exception as e:
            VISITED = set()


def save_visited():
    json.dump(sorted(VISITED), open(STATE_JSON, "w", encoding="utf-8"), ensure_ascii=False)


# ── Crawl incremental ────────────────────────────────────────────────────────
def crawl_domain(domain, quiet=False):
    """Crawl um domínio: fetch via Tor + categoria + idioma + deep-2 children."""
    if domain in VISITED:
        return None
    record = {
        "domain": domain, "url": domain, "status": "error", "title": "",
        "description": "", "category": DEFAULT_CATEGORY, "language": "generic",
        "error": "", "fetched_at": datetime.now(timezone.utc).isoformat(),
        "via_proxy": "tor", "children": [],
    }
    try:
        status, body = tor_fetch(domain)
        if status == 0 and body:
            title, desc = parse_title_description(body)
            record["title"] = title[:300]
            record["description"] = desc[:500]
            record["language"] = detect_language(title + " " + desc)
            record["category"] = _categorize(title, desc)
            if is_routing_site(title, desc) and record["category"] == DEFAULT_CATEGORY:
                record["category"] = "generic"
            record["status"] = "success"
            record["via_proxy"] = "tor"
            # Hidden wiki: children + grandchildren (depth 2) via LinkTree do Trobot
            if record["category"] == "wiki" or "wiki" in (title + desc).lower():
                record["children"] = follow_wiki_children(domain, depth=2)
    except Exception as e:
        record["error"] = str(e)
    VISITED.add(domain)
    if not quiet:
        mark = "✓" if record["status"] == "success" else "✗"
        print(f"  {mark} {domain} [{record.get('category','?')}/{record.get('language','?')}]", flush=True)
    return record


def crawl_incremental(domains, quiet=False, limit=None, progress_batch=5):
    """Crawl sequencial (1 thread), acrescentando apenas domínios novos."""
    if limit:
        domains = domains[:limit]
    domains = [d for d in domains if d not in VISITED]
    os.makedirs(OUT_DIR, exist_ok=True)
    fieldnames = ["domain", "url", "status", "title", "description", "category",
                  "language", "error", "fetched_at", "via_proxy", "children"]
    if not os.path.exists(CSV_PATH):
        csv.DictWriter(open(CSV_PATH, "w", newline="", encoding="utf-8"),
                       fieldnames=fieldnames, extrasaction="ignore").writeheader()
    results = []
    completed = 0
    for i, domain in enumerate(domains, 1):
        rec = crawl_domain(domain, quiet=quiet)
        if rec:
            results.append(rec)
        completed += 1
        if (completed % progress_batch == 0) or completed == len(domains):
            save_visited()
            write_csv(results)
            if not quiet:
                print(f"[INFO] Progresso: {completed}/{len(domains)}", flush=True)
    save_visited()
    write_csv(results)
    return results


def write_csv(results):
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=["domain", "url", "status", "title", "description",
                                       "category", "language", "error", "fetched_at",
                                       "via_proxy", "children"], extrasaction="ignore").writerows(
            results)


# ── Concorrência (teste) ─────────────────────────────────────────────────────
def crawl_concurrent(domains, quiet=False, workers=4, limit=None):
    if limit:
        domains = domains[:limit]
    domains = [d for d in domains if d not in VISITED]
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        f2d = {ex.submit(crawl_domain, d): d for d in domains}
        for fut in as_completed(f2d):
            rec = fut.result()
            if rec:
                results.append(rec)
            if len(results) % 5 == 0:
                save_visited()
                write_csv(results)
    save_visited()
    write_csv(results)
    return results


# ── Relatório (reutiliza o Trobot para análise se disponível) ────────────────
def print_report(results):
    total = len(results)
    success = sum(1 for r in results if r.get("status") == "success")
    cats, langs = {}, {}
    for r in results:
        if r.get("status") == "success":
            cats[r.get("category", "generic")] = cats.get(r.get("category", "generic"), 0) + 1
            langs[r.get("language", "generic")] = langs.get(r.get("language", "generic"), 0) + 1
    print(f"\n=== Relatório ===")
    print(f"Total: {total} | Sucesso: {success} | Erros: {total - success}")
    print(f"Categorias: {dict(sorted(cats.items(), key=lambda x: -x[1]))}")
    print(f"Idiomas: {dict(sorted(langs.items(), key=lambda x: -x[1]))}")


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Crawler .onion via Ahmia + Trobot (incr., 1 thread incial).")
    parser.add_argument("--fast", action="store_true", help="pular URLs já visitadas")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrent", type=int, default=0, help="quantos threads (0 = seqüencial)")
    args = parser.parse_args()

    load_visited(quiet=args.quiet)

    # verificar conexão via Tor
    try:
        c = httpx.Client(proxy=f"socks5://127.0.0.1:9050", timeout=15, verify=False, follow_redirects=True)
        r = c.get("https://check.torproject.org/api/ip", timeout=15)
        m = re.search(r'"IP":\s*"([^"]+)"', r.text)
        print(f"[OK] Tor proxy ativo. exit node: {m.group(1)}" if m else "[WARN] Tor proxy sem IP", flush=True)
        c.close()
    except Exception as e:
        print(f"[ERRO] Tor inativo: {e}", flush=True)
        sys.exit(1)

    # carregar domínios do índice Ahmia ou do catalog.csv
    if os.path.exists(AHMI_CACHE_PATH):
        raw = open(AHMI_CACHE_PATH, encoding="utf-8", errors="ignore").read()
        domains = re.findall(r"([a-z2-7]{56}\.onion)", raw)
        print(f"[OK] {len(set(domains))} domínios carregados do ahmia.", flush=True)
    elif os.path.exists(CSV_PATH):
        domains = [row["domain"] for row in csv.DictReader(open(CSV_PATH, encoding="utf-8"))]
        print(f"[OK] {len(domains)} domínios do catalog.csv.", flush=True)
    else:
        print("[ERRO] sem índice Ahmia nem catalog.", flush=True)
        sys.exit(1)

    domains = list(dict.fromkeys(domains))  # dedup preserva ordem
    if args.fast:
        domains = [d for d in domains if d not in VISITED]

    workers = args.concurrent if args.concurrent > 0 else 1
    if workers > 1:
        results = crawl_concurrent(domains, quiet=args.quiet, workers=workers, limit=args.limit)
        print(f"[INFO] Crawl concorrente: {workers} threads.", flush=True)
    else:
        results = crawl_incremental(domains, quiet=args.quiet, limit=args.limit)
        print("[INFO] Crawl seqüencial (1 thread), incremental.", flush=True)

    save_visited()
    with open(META_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print_report(results)
    if ANALYST_AVAILABLE:
        try:
            from torbot.analyst import build_evidence, build_graph, deterministic_claims
            evidence = build_evidence(results)
            graph = build_graph(results, evidence)
            claims = deterministic_claims(results)
            cat_counts = {}
            for r in results:
                if r.get("status") == "success":
                    cat = r.get("category", "generic")
                    cat_counts[cat] = cat_counts.get(cat, 0) + 1
            report = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "success": sum(1 for r in results if r.get("status") == "success"),
                "categories": dict(sorted(cat_counts.items(), key=lambda x: -x[1])),
                "claims": claims,
            }
            json.dump(report, open(ANALYSIS_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            print(f"[OK] análise gravada em {ANALYSIS_JSON}", flush=True)
        except Exception as e:
            print(f"[WARN] análise falhou: {e}", flush=True)


if __name__ == "__main__":
    main()
