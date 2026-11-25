import sqlite3
conn = sqlite3.connect('/home/ubuntu/torbot-catalog/torbot_v4.db')
cur = conn.cursor()
cur.execute('SELECT domain, status FROM sites WHERE category="forum" OR category="blog" OR category="service"')
rows = cur.fetchall()
print('Categories found:')
for r in rows[:10]:
    print(' ', r)
cur.execute('SELECT domain, title, category, language, parent_domain, depth FROM sites WHERE depth > 0')
rows = cur.fetchall()
print('Recursive results (depth>0):', len(rows))
for r in rows[:10]:
    print(' ', r)
conn.close()
