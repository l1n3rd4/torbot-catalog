import importlib.util
import sys
sys.path.insert(0, "/home/ubuntu/torbot-catalog")
spec = importlib.util.spec_from_file_location("dwebc3", "/home/ubuntu/torbot-catalog/dark-web-crawler-v3.py")
dwebc3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dwebc3)
tor_fetch = dwebc3.tor_fetch
parse_title_description = dwebc3.parse_title_description

url = "http://jieq75a6uwqbj5sjzaxlnd7xwgs35audjmkk4g3gfjwosfrz7cp47xid.onion/index.html"
status, body = tor_fetch(url)
print("URL:", url)
print("STATUS:", status)
print("LEN:", len(body))
if body:
    print("--- first 400 bytes ---")
    print(body[:400].decode("utf-8", errors="replace"))
