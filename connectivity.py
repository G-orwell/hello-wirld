# connectivity.py
import socket
import sys
import urllib.request
from contextlib import closing

TEST_HOST = "example.com"
TEST_URL  = f"https://{TEST_HOST}"
TIMEOUT   = 5.0   # seconds


def http_check() -> bool:
    try:
        with urllib.request.urlopen(TEST_URL, timeout=TIMEOUT) as resp:
            return 200 <= resp.getcode() < 400
    except Exception:
        return False


def dns_check() -> bool:
    try:
        socket.gethostbyname(TEST_HOST)
        return True
    except Exception:
        return False


def socket_check() -> bool:
    try:
        with closing(socket.create_connection((TEST_HOST, 80), timeout=TIMEOUT)):
            return True
    except Exception:
        return False


def run_all() -> dict:
    """Return a dict with the three results."""
    return {
        "http":   http_check(),
        "dns":    dns_check(),
        "socket": socket_check(),
    }


def main() -> None:
    results = run_all()
    print("Internet connectivity test results:")
    for method, ok in results.items():
        print(f"  {method:6}: {'✅ OK' if ok else '❌ FAILED'}")
    if any(results.values()):
        print("\n✅ This process can reach the Internet.")
    else:
        print("\n❌ No outbound connectivity detected.")
    return results


if __name__ == "__main__":
    main()
