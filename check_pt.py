import csv

with open('/home/ubuntu/torbot-catalog/catalog.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    pt_sites = [row for row in reader if (row.get('language') or '').strip().lower() == 'pt']

print(f'Total marcados como pt: {len(pt_sites)}')
print()

# Portuguese-specific words (unique to PT, not Spanish/English)
pt_words_clean = set('''
português português portugal brasil
páginas página você nós nós
fazer fizer fez feito seria
conteúdo artigo notícia
área voltar mesmo mais porque
vamos fique seja clique
faça dê você
nós os as o a
página artigo notícia
conteúdo vôos vôo
homens mulheres homem mulher
todos todas todos todos
sem não não não
só só só só só
pode pode pode
quando quando quando
como como como
onde onde onde
para para para
a a a a a a
o o o o o o
de de de de de
em em em em em
um um um um
uma uma uma uma uma
é é é é
'''.split())

# English words
en_words = set('''
the be and of to in is it you that he she we they
or but as not at an on by for with from are this was have has had
will would could should may might can must
'''.split())

results_pt = []
results_other = []

for row in pt_sites:
    desc = (row.get('description') or '').strip()
    title = (row.get('title') or '').strip()
    d = desc.lower()

    pw = sum(1 for w in pt_words_clean if w in d)
    ec = sum(1 for w in en_words if w in d)

    if pw >= 3 and pw > ec:
        results_pt.append((title, desc, pw, ec))
    else:
        results_other.append((title, desc, pw, ec))

print(f'=== REALMENTE EM PORTUGUÊS (>=3 pt words & pt>en) === {len(results_pt)} sites')
print()
for title, desc, pw, ec in results_pt:
    print(f'  - {title[:65]}')
    print(f'    pt_words={pw} en_words={ec}')
    print(f'    desc: {desc[:100]}')
print()
print(f'=== OUTROS: {len(results_other)} ===')
for title, desc, pw, ec in results_other:
    print(f'  - {title[:65]} (pt={pw}, en={ec})')
