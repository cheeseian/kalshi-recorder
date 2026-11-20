"""Minimal read-only Kalshi trade-api v2 client (request signing only). The recorder never places orders."""
import base64
import os
import time
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding

DEFAULT_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def _load_private_key():
    pem = os.environ.get("KALSHI_PRIVATE_KEY")
    path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if not pem and path and os.path.exists(path):
        with open(path) as f:
            pem = f.read()
    if not pem:
        return None
    # Env-var editors often flatten newlines; accept literal "\n" too.
    pem = pem.replace("\\n", "\n").strip() + "\n"
    return serialization.load_pem_private_key(pem.encode(), password=None)


class KalshiClient:
    def __init__(self, base=None):
        self.base = (base or os.environ.get("KALSHI_API_BASE") or DEFAULT_BASE).rstrip("/")
        self.prefix = urlparse(self.base).path
        self.key_id = os.environ.get("KALSHI_API_KEY_ID")
        self._key = _load_private_key()
        self.session = requests.Session()

    @property
    def authed(self):
        return bool(self.key_id and self._key)

    def _auth_headers(self, method, path):
        ts = str(int(time.time() * 1000))
        msg = (ts + method + self.prefix + path).encode()
        if isinstance(self._key, ed25519.Ed25519PrivateKey):
            sig = self._key.sign(msg)
        else:  # RSA-PSS
            sig = self._key.sign(
                msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

    def _req(self, method, path, params=None, body=None):
        for attempt in range(6):
            headers = {"Content-Type": "application/json"}
            if self.authed:
                headers.update(self._auth_headers(method, path))
            try:
                r = self.session.request(method, self.base + path, params=params, json=body,
                                         headers=headers, timeout=20)
            except (requests.ConnectionError, requests.Timeout):
                # transient network/read timeouts used to abort the whole cycle; retry like a 429
                if attempt == 5:
                    raise
                time.sleep(0.5 * 2 ** attempt)
                continue
            if r.status_code != 429:
                break
            time.sleep(float(r.headers.get("Retry-After") or 0.5 * 2 ** attempt))
        r.raise_for_status()
        return r.json() if r.content else {}

