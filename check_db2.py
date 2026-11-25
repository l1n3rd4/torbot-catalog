import sqlite3
conn = sqlite3.connect('/home/ubuntu/torbot-catalog/torbot_v4.db')
cur = conn.cursor()
# Check pending sites
cur.execute('SELECT COUNT(*) FROM sites WHERE status="pending"')
print('Pending sites:', cur.fetchone()[0])
# Check recent sites (sorted by fetched_at desc)
cur.execute('SELECT domain, status, category, language FROM sites ORDER BY fetched_at DESC LIMIT 20')
rows = cur.fetchall()
print('\nRecent sites:')
for r in rows:
    print(' ', r)
conn.close()
