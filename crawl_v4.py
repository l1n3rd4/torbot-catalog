#!/usr/bin/env python3
"""
crawl_v4.py — Novo crawler .onion com SQLite, descoberta recursiva e idiomas/categorias melhorados.

Novas funcionalidades:
  1. SQLite como base de dados (URL como primary key, índice único).
  2. Descoberta recursiva: segue links dentro de sites conhecidos.
  3. Detecção de português brasileiro melhorada (múltiplas métricas).
  4. Categorias melhoradas (incluindo fóruns, blogs, serviços, etc.).
  5. Threads com ThreadPoolExecutor (--concurrent N).
  6. Migração automática do catalog.csv para SQLite.
  7. Salvamento incremental (visited set no SQLite).

Uso:
  python3 crawl_v4.py --import-csv        # migrate catalog.csv -> torbot_v4.db
  python3 crawl_v4.py --limit 10          # teste com 10 sites
  python3 crawl_v4.py --concurrent 4      # concorrência com 4 threads
  python3 crawl_v4.py --concurrent 4 --limit 10  # teste com 10 sites
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import socket
import ssl
import struct
import subprocess
import sys
import time
import threading
import http.client
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlsplit

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
except ImportError as e:
    print(f"[ERRO] Dependência faltando: {e}", flush=True)
    sys.exit(1)

TORBOT_SRC_DIR = os.path.dirname(TORBOT_SRC)
try:
    from torbot.analyst import build_evidence, build_graph, deterministic_claims
    ANALYST_AVAILABLE = True
except ImportError:
    ANALYST_AVAILABLE = False

try:
    from torbot.modules.color import color
    FROM_COLOR = True
except Exception:
    FROM_COLOR = False

OUT_DIR = "/home/ubuntu/torbot-catalog"
DB_PATH = os.path.join(OUT_DIR, "torbot_v4.db")
CSV_PATH = os.path.join(OUT_DIR, "catalog.csv")
META_JSON = os.path.join(OUT_DIR, "results_v4.json")
STATE_JSON = os.path.join(OUT_DIR, "visited_v4.json")
TIMEOUT = 40
REQUEST_DELAY = 0.8
DEFAULT_CATEGORY = "generic"
AHMI_CACHE_PATH = os.path.expanduser("~/ahmia_index.html")
TOR_SOCKS = "127.0.0.1:9050"

# ── Lock para acesso ao SQLite ────────────────────────────────────────────────
DB_LOCK = threading.Lock()

# ── Categorias melhoradas ──────────────────────────────────────────────────────
CATEGORIES = [
    ("streaming", "Streaming", ["stream", "video", "player", "yt", "plex", "jellyfin", "emby", "vod", "netflix"]),
    ("wiki", "Wiki", ["wiki", "encyclopedia", "knowledge", "articles", "corpus"]),
    ("forum", "Fórum / Forum", ["forum", "fórum", "talk", "discuss", "board", "thread", "threaded", "reply", "post", "guestbook"]),
    ("marketplace", "Marketplace", ["market", "shop", "store", "cart", "buy", "sell", "payment", "crypto", "bitcoin", "tmall"]),
    ("blog", "Blog / CMS", ["blog", "notícias", "news", "post", "article", "diary", "journal", "entry"]),
    ("service", "Serviço / Ferramenta", ["serviço", "service", "tool", "software", "hosting", "vps", "domicílio", "email", "certificado"]),
    ("generic", "Genérico", []),
]

ROUTE_WORDS = ["index", "home", "login", "register", "sign", "status", "about", "contact", "help", "support"]


# ── Fetch via túnel SOCKS5 (coração da correção) ─────────────────────────────
def _socks5_connect(host, port=443, timeout=40):
    """Conecta ao Tor SOCKS5 como o curl --socks5-hostname."""
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


def tor_fetch(url, retries=1):
    """Obtém o HTML de um .onion/URL via Tor."""
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
            conn.request("GET", "/", headers={"User-Agent": "TorbotCatalog/4.0"})
            resp = conn.getresponse()
            data = resp.read()
            ss.close()
            s.close()
            return resp.status, data, ""
        except Exception as e:
            last_err = e
            time.sleep(REQUEST_DELAY * (attempt + 1))
    cmd = ["curl", "--socks5-hostname", "127.0.0.1:9050", "-sS", "-L", "--max-time", str(TIMEOUT), url]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=TIMEOUT + 5)
        err = r.stderr.decode("utf-8", errors="ignore").strip()
        return (r.returncode, r.stdout, err)
    except Exception as e2:
        return (-1, (f"fallback-failed: {e2}; raw: {last_err}").encode(), "")


# ── Extras de título/descrição ────────────────────────────────────────────────
def parse_title_description(html_bytes):
    text = html_bytes.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html_bytes, "html.parser")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    title = (soup.title.text.strip() if soup.title else "")
    body = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    return title, body[:1000]


# ── Detecção de português brasileiro melhorada ────────────────────────────────
def detect_language_pt(text):
    """Detecta português brasileiro com métricas mais robustas."""
    if not text:
        return "generic"
    lower = text.lower()
    # Métrica 1: palavras portuguesas (excluindo falsos positivos em espanhol)
    pt_words = [
        " o ", " a ", " de ", " do ", " da ", " que ", " para ", " com ", " como ",
        " e ", " ou ", " mas ", " ser ", " tem ", " sem ", " sua ", " nossa ", " seu ",
        " é ", " há ", " você ", " eles ", " elas ", " este ", " essa ", " aquele ",
        " aqui ", " hoje ", " amanhã ", " se ", " não ", " sim ", " eu ", " tu ", " ele ",
        " ela ", " nós ", " vós ", " eles ", " elas ", " mim ", " ti ", " si ",
        "meu ", " sua ", " nosso ", " seu ", " minha ", " sua ", " nossa ", " nossa ",
        "isto ", " isso ", " aquilo ", " este ", " essa ", " aquele ", " aquela ",
        "estes ", " estes ", " aqueles ", " aquelas ", "qual ", "quem ", "onde ",
        "quando ", "porque ", "como ", "seja ", "ser ", "estar ", "ter ", "haver ",
        "fazer ", "dar ", "poder ", "saber ", "querer ", "ir ", "ver ", "passar ",
        "viver ", "perceber ", "pensar ", "crer ", "acreditar ", "avaliar ", "considerar ",
        "estimar ", "visitar ", "conhecer ", "navegar ", "explorar ", "buscar ",
        "encontrar ", "localizar ", "descobrir ", "revelar ", "mostrar ", "exibir ",
        "apresentar ", "demonstrar ", "evidenciar ", "constatar ", "observar ", "notar ",
        "palpar ", "tocar ", "ouvir ", "enquanto ", "assim ", "também ", "ainda ",
        "mesmo ", "só ", "até ", "somente ", "apenas ", "pouco ", "muito ", "mais ",
        "menos ", "tanto ", "qualquer ", "todo ", "toda ", "todos ", "todas ", "cada ",
        "outro ", "outra ", "outros ", "outras ", "também ", "ainda ", "mesmo ", "só ",
        "até ", "somente ", "apenas ", "pouco ", "muito ", "mais ", "menos ", "tanto ",
        "qualquer ", "qualquer ", "todo ", "toda ", "todos ", "todas ", "cada ", "outro ",
        "outra ", "outros ", "outras",
    ]
    pt_count = sum(1 for w in pt_words if w in lower)
    # Métrica 2: caracteres acentuados (pt, não espanhol)
    accented = set("ãõáéíóúâêôç")
    pt_accents = sum(1 for c in lower if c in accented)
    # Métrica 3: diferença entre pt e espanhol
    # Espanhol: "ñ" é comum, "á" também. Pt: "ã", "õ", "ç", "é", "ã"
    es_words = [" el ", " la ", " los ", " las ", " es ", " en ", " de ", " para ",
                " con ", " por ", " que ", " como ", " más ", " muy ", " esta ", " está ",
                " y ", " o ", " al ", " sino ", " una ", " unos ", " unas ", " quien ", " porque "]
    es_count = sum(1 for w in es_words if w in lower)
    en_words_pt = [
        " the ", " and ", " is ", " in ", " to ", " of ", " it ", " you ", " that ",
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
        " down ", " day ", " did ", " get ", " come ", " made ", " may ", " part ",
    ]
    en_count = sum(1 for w in en_words_pt if w in lower)

    if pt_accents >= 3:
        return "pt"
    if pt_count >= 6 and pt_count > en_count * 2:
        return "pt"
    if en_count >= 6 and en_count > pt_count * 2:
        return "en"
    if pt_accents >= 2:
        return "pt"
    return "generic"


# ── Categoria + roteamento ────────────────────────────────────────────────────
def _categorize(title, description):
    text = (title + " " + description).lower()
    if not text:
        return DEFAULT_CATEGORY
    for key, name, left_kw in CATEGORIES:
        left_list = left_kw if isinstance(left_kw, list) else [left_kw]
        left_hit = any(kw in text for kw in left_list)
        if key == "generic":
            continue
        if left_hit:
            return key
    # Verifica categoria de fórum via descrição
    if any(w in text for w in ["fórum", "forum", "board", "thread", "post", "guestbook", "discuss"]):
        return "forum"
    return DEFAULT_CATEGORY


def is_routing_site(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in ROUTE_WORDS)


# ── Trobot LinkTree (árvore de filhos do hidden wiki) ────────────────────────
def follow_wiki_children(domain, depth=2, max_children=10):
    """Usa o LinkTree do Trobot para percorrer children e filhos(de filhos)."""
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


# ── Estado incremental (SQLite) ───────────────────────────────────────────────
def init_db():
    """Cria o banco SQLite e a tabela principal."""
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sites (
            domain TEXT PRIMARY KEY,
            url TEXT UNIQUE,
            status TEXT DEFAULT 'pending',
            title TEXT DEFAULT '',
            description TEXT DEFAULT '',
            category TEXT DEFAULT 'generic',
            language TEXT DEFAULT 'generic',
            error TEXT DEFAULT '',
            fetched_at TEXT,
            via_proxy TEXT DEFAULT 'tor',
            children TEXT DEFAULT '[]',
            discovered_at TEXT DEFAULT (datetime('now')),
            parent_domain TEXT,
            depth INTEGER DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON sites(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_language ON sites(language)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_category ON sites(category)")
    conn.commit()
    return conn


def load_visited_set(conn):
    """Carrega domínios já visitados do SQLite."""
    cur = conn.cursor()
    cur.execute("SELECT domain FROM sites WHERE status IN ('success', 'error')")
    return set(row[0] for row in cur.fetchall())


def save_visited_set(conn, domains):
    """Salva domínios visitados no SQLite."""
    cur = conn.cursor()
    fetched_at = datetime.now(timezone.utc).isoformat()
    for d in domains:
        cur.execute(
            "INSERT OR IGNORE INTO sites (domain, url, status, title, description, category, language, error, fetched_at, via_proxy, children, discovered_at, parent_domain, depth) VALUES (?, ?, 'success', '', '', '', '', '', ?, 'tor', '[]', ?, '', 0)",
            (d, d, fetched_at, fetched_at)
        )


def insert_site(conn, domain, url, status="pending", title="", description="",
                category="generic", language="generic", error="",
                fetched_at=None, via_proxy="tor", children="[]", parent_domain=None, depth=0):
    """Insere ou atualiza um site. URL é a primary key."""
    cur = conn.cursor()
    if fetched_at is None:
        fetched_at = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT OR REPLACE INTO sites
        (domain, url, status, title, description, category, language, error, fetched_at, via_proxy, children, discovered_at, parent_domain, depth)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (domain, url, status, title, description, category, language, error, fetched_at, via_proxy, children, fetched_at, parent_domain, depth))
    conn.commit()


def insert_batch(conn, sites):
    """Insere múltiplos sites de uma vez."""
    cur = conn.cursor()
    for s in sites:
        insert_site(conn, **s)
    conn.commit()


# ── Crawl ─────────────────────────────────────────────────────────────────────
def crawl_domain(domain, conn=None, depth=0):
    """Crawl um domínio via Tor e salva no SQLite."""
    if conn:
        with DB_LOCK:
            pass  # lock não é necessário dentro de transações

    record = {
        "domain": domain, "url": domain, "status": "error", "title": "",
        "description": "", "category": DEFAULT_CATEGORY, "language": "generic",
        "error": "", "fetched_at": datetime.now(timezone.utc).isoformat(),
        "via_proxy": "tor", "children": "[]", "parent_domain": None, "depth": depth,
    }
    try:
        status, body, err = tor_fetch(domain)
        if status == 0 and body:
            title, desc = parse_title_description(body)
            record["title"] = title[:300]
            record["description"] = desc[:500]
            record["language"] = detect_language_pt(title + " " + desc)
            record["category"] = _categorize(title, desc)
            if is_routing_site(title, desc) and record["category"] == DEFAULT_CATEGORY:
                record["category"] = "generic"
            record["status"] = "success"
            record["via_proxy"] = "tor"
            # Hidden wiki: children + grandchildren (depth 2) via LinkTree
            if record["category"] == "wiki" or "wiki" in (title + desc).lower():
                children_list = follow_wiki_children(domain, depth=2)
                record["children"] = json.dumps(children_list)
        else:
            record["status"] = "error"
            record["error"] = err or f"curl exit code {status}, empty body"
    except Exception as e:
        record["error"] = str(e)

    # Salva no SQLite
    insert_site(conn, **record)
    return record


def crawl_batch(domains, conn, workers=1, batch_size=10, force=False):
    """Crawl um lote de domínios (seqüencial ou concorrente)."""
    if not force:
        domains = [d for d in domains if d not in load_visited_set(conn)]
    if not domains:
        return []

    results = []
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            f2d = {ex.submit(crawl_domain, d, conn): d for d in domains}
            for fut in as_completed(f2d):
                rec = fut.result()
                if rec:
                    results.append(rec)
    else:
        for d in domains:
            rec = crawl_domain(d, conn)
            if rec:
                results.append(rec)

    # Salva progresso
    visited = load_visited_set(conn)
    save_visited_set(conn, visited)
    return results


# ── Descoberta recursiva ──────────────────────────────────────────────────────
def discover_recursive(conn, domain, depth=0, max_depth=2, limit=3):
    """Descobre novos sites a partir de um site conhecido."""
    if depth >= max_depth:
        return []

    # Obtém links do site
    try:
        status, body, err = tor_fetch(domain)
        if status != 0 or not body:
            return []

        # Extrai links .onion
        text = body.decode("utf-8", errors="ignore")
        links = re.findall(r'([a-z2-7]{56}\.onion)', text)
        links = list(dict.fromkeys(links))  # dedup
        links = [f"http://{l}" for l in links]
        # Filtra links já visitados
        visited = load_visited_set(conn)
        new_links = [l for l in links if l not in visited]

        results = []
        for link in new_links[:limit]:
            rec = crawl_domain(link, conn, depth=depth+1)
            results.append(rec)
            if rec["status"] == "success":
                # Descobre recursivamente
                child_results = discover_recursive(conn, link, depth + 1, max_depth, limit)
                results.extend(child_results)

        return results
    except Exception as e:
        print(f"[WARN] Descoberta falhou em {domain}: {e}", flush=True)
        return []


# ── Migração de catalog.csv para SQLite ──────────────────────────────────────
def migrate_csv_to_sqlite(csv_path, db_path):
    """Migra o catalog.csv existente para o banco SQLite."""
    print(f"[INFO] Migrando {csv_path} -> {db_path}...", flush=True)
    conn = init_db()
    cur = conn.cursor()

    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            domain = row.get('domain', '').strip()
            url = row.get('url', domain).strip()
            if not domain:
                continue

            status = row.get('status', 'error')
            title = row.get('title', '').strip()
            desc = row.get('description', '').strip()
            category = row.get('category', '').strip()
            language = row.get('language', '').strip()
            error = row.get('error', '').strip()
            fetched_at = row.get('fetched_at', datetime.now(timezone.utc).isoformat())
            via_proxy = row.get('via_proxy', 'tor')
            children = row.get('children', '[]')

            cur.execute("""
                INSERT OR REPLACE INTO sites
                (domain, url, status, title, description, category, language, error, fetched_at, via_proxy, children)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (domain, url, status, title, desc, category, language, error, fetched_at, via_proxy, children))
            count += 1

            if count % 1000 == 0:
                conn.commit()
                print(f"[INFO] {count} linhas migradas...", flush=True)

    conn.commit()
    print(f"[OK] Migração concluída: {count} registros.", flush=True)
    conn.close()
    return count


# ── Relatório ─────────────────────────────────────────────────────────────────
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
    parser = argparse.ArgumentParser(description="Crawler .onion v4 com SQLite, descoberta recursiva e idiomas/categorias melhorados.")
    parser.add_argument("--import-csv", action="store_true", help="Migra catalog.csv para o SQLite")
    parser.add_argument("--fast", action="store_true", help="Pular URLs já visitadas")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--limit", type=int, default=10, help="Máximo de sites por lote")
    parser.add_argument("--concurrent", type=int, default=1, help="Threads (1 = seqüencial)")
    parser.add_argument("--discover", type=str, default=None, help="Domínio inicial para descoberta recursiva")
    parser.add_argument("--max-depth", type=int, default=2, help="Profundidade máxima de descoberta")
    parser.add_argument("--discover-limit", type=int, default=3, help="Máximo de filhos por nível")
    args = parser.parse_args()

    # Inicializa o banco
    conn = init_db()
    visited = load_visited_set(conn)

    # Importa catalog.csv se solicitado
    if args.import_csv:
        migrate_csv_to_sqlite(CSV_PATH, DB_PATH)
        # Recarrega o visited set após migração
        visited = load_visited_set(conn)

    # Verificar conexão via Tor
    try:
        c = httpx.Client(proxy=f"socks5://127.0.0.1:9050", timeout=40, verify=False, follow_redirects=True)
        r = c.get("https://check.torproject.org/api/ip", timeout=40)
        m = re.search(r'"IP":\s*"([^"]+)"', r.text)
        print(f"[OK] Tor proxy ativo. exit node: {m.group(1)}" if m else "[WARN] Tor proxy sem IP", flush=True)
        c.close()
    except Exception as e:
        print(f"[ERRO] Tor inativo: {e}", flush=True)
        sys.exit(1)

    # Verificar domínios do índice Ahmia
    if os.path.exists(AHMI_CACHE_PATH):
        raw = open(AHMI_CACHE_PATH, encoding="utf-8", errors="ignore").read()
        domains = re.findall(r"([a-z2-7]{56}\.onion)", raw)
        print(f"[OK] {len(set(domains))} domínios carregados do ahmia.", flush=True)
        domains = list(dict.fromkeys(domains))
    elif os.path.exists(CSV_PATH):
        domains = [row["domain"] for row in csv.DictReader(open(CSV_PATH, encoding="utf-8"))]
        print(f"[OK] {len(domains)} domínios do catalog.csv.", flush=True)
    else:
        print("[ERRO] sem índice Ahmia nem catalog.", flush=True)
        sys.exit(1)

    # Filtra já visitados (se --fast)
    if args.fast:
        domains = [d for d in domains if d not in visited]
    else:
        # Se --discover, substitui a lista por apenas o domínio informado
        # (mesmo que já exista na base, força o recrawl para análise)
        if args.discover:
            domains = [args.discover]

    print(f"[INFO] Domínios a processar: {len(domains)}", flush=True)

    # Descoberta recursiva se especificado
    if args.discover:
        print(f"[INFO] Iniciando descoberta recursiva em {args.discover}...", flush=True)
        results = crawl_batch([args.discover], conn, workers=args.concurrent, batch_size=args.limit, force=True)
        if results and results[0].get('status') == 'success':
            print(f"[INFO] Descobrindo recursivamente... (max depth={args.max_depth})", flush=True)
            recursive_results = discover_recursive(conn, args.discover, depth=0, max_depth=args.max_depth, limit=args.discover_limit)
            results.extend(recursive_results)
        print(f"[INFO] Descoberta concluída. Total: {len(results)}", flush=True)
        # Salva resultados
        with open(META_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print_report(results)
        conn.close()
        return

    # Crawl normal
    total_selected = min(len(domains), args.limit)
    print(f"[INFO] Processando {total_selected} domínios...", flush=True)
    results = crawl_batch(domains[:total_selected], conn, workers=args.concurrent, batch_size=total_selected)
    print(f"[INFO] Crawl concluído: {len(results)} resultados.", flush=True)

    with open(META_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print_report(results)

    # Fecha conexão
    conn.close()


if __name__ == "__main__":
    main()
