import httpx
import http.client
import socket
import ssl
import struct

class TorTransport(httpx.BaseTransport):
    def __init__(self, sock_host="127.0.0.1", sock_port=9050, timeout=15, targets=()):
        self.socks_host = sock_host
        self.socks_port = sock_port
        self.timeout = timeout
        self.targets = set(targets)

    def handle_request(self, request):
        host = request.url.host
        port = request.url.port or 80
        sock = self._connect(host, port)
        ss = None
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ss = ctx.wrap_socket(sock, server_hostname=host)
            conn = http.client.HTTPConnection(host, port=port, timeout=self.timeout)
            conn.sock = ss
            conn.request(request.method, request.url.path or "/", headers=dict(request.headers))
            resp = conn.getresponse()
            r = resp.read()
            return httpx.Response(resp.status, request=request, content=r, headers=resp.headers)
        finally:
            try:
                if ss is not None:
                    ss.close()
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass

    def _connect(self, host, port):
        s = socket.create_connection((self.socks_host, self.socks_port), timeout=self.timeout)
        s.settimeout(self.timeout)
        s.sendall(bytes([5, 2, 0, 2]))
        s.recv(2)
        s.sendall(bytes([5, 2, 0, 0, 0, 0]))
        s.recv(4)
        dom = host.encode()
        req = bytes([5, 1, 0, 3]) + bytes([len(dom)]) + dom + struct.pack(">H", port)
        s.sendall(req)
        resp = s.recv(4)
        if resp[3] == 3:
            l = s.recv(1)[0]
            s.recv(l)
        elif resp[3] == 1:
            s.recv(6)
        elif resp[3] == 4:
            s.recv(18)
        s.recv(2)
        return s
