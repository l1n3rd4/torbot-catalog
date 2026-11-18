#!/usr/bin/env python3
"""
extrair_brasileiro.py — Extrai sites em português (PT/Brasil)
do catálogo de sites .onion.

Recursos:
  • Streaming JSON (ijson) — não carrega o arquivo inteiro na memória
  • Filtra por idioma detectado (language = "pt")
  • Usa palavras-chave para detectar português em títulos/descrições
  • Saída: terminal + JSON limpo

Uso:
    /home/ubuntu/tools/TorBot/.venv/bin/python extrair_brasileiro.py
"""

import json
import re
import sys
import os

try:
    import ijson
except ImportError:
    print("Erro: ijson não encontrado.")
    print("Instale com: /home/ubuntu/tools/TorBot/.venv/bin/pip install ijson")
    sys.exit(1)

# Caminhos
RESULTS_JSON = os.path.expanduser("~/torbot-catalog/results.json")
CSV_PATH = os.path.expanduser("~/torbot-catalog/catalog.csv")
OUTPUT_JSON = os.path.expanduser("~/torbot-catalog/brasileiro.json")

# Palavras-chave de português (altas + baixas)
PT_KEYWORDS = [
    # Artigos e pronomes
    "o ", "a ", "o", "a", "os", "as", "me ", "te ", "se ", "nos ", "vos ",
    "ele ", "ela ", "eles ", "elas ", "nós ", "você ", "vocês ", "tu ", "lhe ",
    "este ", "essa ", "aquele ", "aquela ", "isto ", "isso ", "aquilo ",
    # Preposições
    "de ", "do ", "da ", "na ", "num ", "em ", "por ", "para ", "com ", "sen ",
    "so ", "entre ", "durante ", "até ", "contra ", "pel ", "pela ", "pela ",
    "sem ", "mais ", "sob ", "traz ", "emb ", "à ", "pelas ", "pelos ",
    # Conectivos
    "e ", "ou ", "mas ", "porém ", "mesmo ", "enquanto ", "apesar ", "embora ",
    "pois ", "logo ", "portanto ", "assim ", "do mesmo ", "tanto ", "tanto ",
    "tanto ", "tanto ", "quer ", "quando ", "onde ", "enquanto ", "desde ",
    # Verbos (infinitivos e particípios)
    "ser ", "sendo ", "houver ", "ter ", "tendo ", "poder ", "pode ", "pode",
    "fazer ", "fazendo ", "vir ", "vindo ", "ir ", "indo ", "ver ", "vendo ",
    "dizer ", "dizendo ", "poder ", "podendo ", "ter ", "tendo ", "ser ",
    "sendo ", "querer ", "querendo ", "saber ", "sabendo ", "dever ", "devedo ",
    "dar ", "dando ", "estar ", "estando ", "decidir ", "decidido ", "usar ",
    "usando ", "perguntar ", "perguntando ", "ensinar ", "ensinando ", "ajudar ",
    "ajudando ", "pensar ", "pensando ", "considar ", "considerando ", "mostrar ",
    "mostrando ", "explicar ", "explicando ", "continuar ", "continuando ",
    # Artigos definidos e indefinidos
    "os ", "as ", "um ", "uma ", "uns ", "umas ", "da ", "do ", "das ", "dos ",
    # Adjetivos comuns
    "bom ", "boa ", "bons ", "boas ", "mau ", "mau ", "novo ", "nova ", "novos ", "novas ",
    "antigo ", "antiga ", "grande ", "grandes ", "pequeno ", "pequena ", "pequenos ", "pequenas ",
    "muito ", "muita ", "muitos ", "muitas ", "mais ", "mais ", "menos ", "menos ",
    "bom ", "ou ", "liso ", "lisas ", "fácil ", "fáceis ", "dificil ", "dificíeis ",
    # Pronomes relativos
    "que ", "quem ", "cujo ", "aquel ", "onde ", "como ",
]

# Palavras em inglês (para descontar)
EN_KEYWORDS = [
    "the ", "and ", "is ", "in ", "to ", "of ", "it ", "you ", "that ", "he ",
    "was ", "for ", "on ", "are ", "with ", "as ", "i ", "his ", "they ", "be ",
    "at ", "one ", "have ", "this ", "from ", "or ", "had ", "by ", "not ", "what ",
    "all ", "were ", "we ", "when ", "your ", "can ", "said ", "there ", "use ",
    "an ", "each ", "which ", "she ", "do ", "how ", "their ", "if ", "will ",
    "up ", "other ", "about ", "out ", "many ", "then ", "them ", "these ",
    "so ", "some ", "her ", "would ", "make ", "like ", "him ", "into ", "time ",
    "has ", "look ", "two ", "more ", "write ", "go ", "see ", "number ", "no ",
    "way ", "could ", "people ", "my ", "than ", "first ", "water ", "been ",
    "call ", "who ", "oil ", "its ", "now ", "find ", "long ", "down ", "day ",
    "did ", "get ", "come ", "made ", "may ", "part ",
]

# Dispositivos que indicam site brasileiro/português
BR_SIGNALS = ["br ", "br.", "br-", "de brasil", "do brasil", "brasil ", "brasil"]

def has_portuguese_keywords(text):
    """Verifica se o texto contém palavras típicas do português."""
    if not text:
        return False
    lower = text.lower()
    pt_count = sum(1 for kw in PT_KEYWORDS if kw in lower)
    en_count = sum(1 for kw in EN_KEYWORDS if kw in lower)
    # Se tem muitos termos pt e poucos em, provavelmente é pt
    return pt_count >= 3 and pt_count > en_count * 2


def detect_portuguese(title, description):
    """Detecta se o site é em português baseado em meta dados."""
    if not title and not description:
        return False

    combined = (title or "") + " " + (description or "")

    # Checar palavras-chave de português
    if has_portuguese_keywords(combined):
        return True

    # Verificar sinais de Brasil
    for signal in BR_SIGNALS:
        if signal in combined.lower():
            return True

    return False


def extract_brazilians(json_path, min_matches=1, limit=None):
    """
    Extrai sites em português do JSON usando ijson (streaming).
    Não carrega o arquivo inteiro na memória.
    """
    resultados = []
    count = 0

    print(f"📖 Ler {json_path} (streaming ijson)...")
    print(f"   Filtro: idioma=pt (ou {min_matches}x palavras-chave pt)")

    with open(json_path, "rb") as f:
        # Parser de streaming
        parser = ijson.parse(f)

        current_item = None
        current_field = ""
        current_value = ""

        # Para rastrear o estado dentro do JSON
        # Stack de campos para rastrear o structure
        field_stack = []

        # ijson.parse retorna (prefix, event, value)
        # Preparamos um loop sobre os eventos
        items = ijson.items(f, "")

        for item in items:
            if limit and count >= limit:
                break

            if isinstance(item, dict):
                # Era um objeto completo (registro)
                # Verificar se é um site com data
                if "domain" in item and "language" in item:
                    language = item.get("language", "").lower()
                    title = item.get("title", "")
                    description = item.get("description", "")

                    # Filtro 1: idioma detectado como pt
                    if language == "pt":
                        resultados.append(item)
                        count += 1
                        print(f"  ✅ {item['domain']}")
                        continue

                    # Filtro 2: palavras-chave de português
                    combined = (title or "") + " " + (description or "")
                    if has_portuguese_keywords(combined):
                        resultados.append(item)
                        count += 1
                        print(f"  ✅ {item['domain']} (palavras-pt)")
                        continue

                    # Filtro 3: sinais de Brasil
                    if detect_portuguese(title, description):
                        resultados.append(item)
                        count += 1
                        print(f"  ✅ {item['domain']} (sinal-br)")
                        continue

                    # Outro: quebra de regra, registrar não-pt
                    # (opcional, não imprime como erro)

    return resultados


def main():
    if not os.path.exists(RESULTS_JSON):
        print(f"❌ Arquivo não encontrado: {RESULTS_JSON}")
        print("   Rode o crawler primeiro.")
        sys.exit(1)

    total_size = os.path.getsize(RESULTS_JSON)
    print(f"📂 {RESULTS_JSON}")
    print(f"   Tamanho: {total_size:,} bytes ({total_size/1024:.1f} KB)")
    print()

    # Extrair sites em português
    brasileiros = extract_brazilians(RESULTS_JSON, min_matches=2, limit=None)

    # Mostrar resultados
    print(f"\n{'='*70}")
    print(f"📊 Resultado: {len(brasileiros)} sites em português encontrados")
    print(f"{'='*70}")

    if not brasileiros:
        print("❌ Nenhum site em português encontrado.")
        print("   Tente verificar o idioma detectado ou ajustar a função de detecção.")
        return

    # Salvar resultados em JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(brasileiros, f, ensure_ascii=False, indent=2)
    print(f"💾 Salvos em: {OUTPUT_JSON}")

    # Mostrar detalhes
    print(f"\n{'='*70}")
    print("📋 Detalhes dos sites encontrados:")
    print(f"{'='*70}")

    for i, site in enumerate(brasileiros, 1):
        print(f"\n[{i}] {site.get('domain', 'N/A')}")
        print(f"    Título: {site.get('title', 'N/A')[:120]}")
        print(f"    Categoria: {site.get('category', 'N/A')}")
        print(f"    Idioma: {site.get('language', 'N/A')}")
        desc = site.get('description', '')
        print(f"    Descrição: {desc[:120] if desc else 'N/A'}")

    # Estatísticas
    print(f"\n{'='*70}")
    print("📈 Estatísticas")
    print(f"{'='*70}")
    langs = {}
    for r in brasileiros:
        lang = r.get("language", "unknown")
        langs[lang] = langs.get(lang, 0) + 1
    print(f"Distribuição de idiomas: {langs}")

    # Resumo do arquivo original
    total_items = 0
    with open(RESULTS_JSON, "rb") as f:
        # Count using ijson
        items_count = 0
        for _ in ijson.items(f, ""):
            items_count += 1
    print(f"Total de itens no JSON original: {items_count:,}")
    print(f"Taxa de filtragem: {len(brasileiros)/max(items_count,1)*100:.1f}%")


if __name__ == "__main__":
    main()
