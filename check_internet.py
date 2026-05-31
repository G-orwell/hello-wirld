#!/usr/bin/env python3
"""
Check Internet connectivity from a hosted Python process.

- Tries an HTTP GET to https://example.com (fast, low‑bandwidth).
- Performs a DNS lookup for a well‑known host.
- Opens a raw TCP socket to port 80 of the same host.

If any of the three succeeds, we consider the environment “Internet‑enabled”.
"""

import socket
import sys
import urllib.request
from contextlib import closing

# ----------------------------------------------------------------------
# Configuration – you can change the test host / URL if you prefer
# ----------------------------------------------------------------------
TEST_HOST = "example.com"
TEST_URL  = f"https://{TEST_HOST}"
TIMEOUT   = 5.0   # seconds


def http_check() -> bool:
    """Attempt a lightweight HTTP GET request."""
    try:
        with urllib.request.urlopen(TEST_URL, timeout=TIMEOUT) as resp:
            # A 2xx/3xx response means we reached the server.
            return 200 <= resp.getcode() < 400
    except Exception as e:
        # Uncomment the next line for debugging:
        # print(f"HTTP check failed: {e}", file=sys.stderr)
        return False


def dns_check() -> bool:
    """Resolve the host name to an IP address."""
    try:
        socket.gethostbyname(TEST_HOST)
        return True
    except Exception as e:
        # print(f"DNS check failed: {e}", file=sys.stderr)
        return False


def socket_check() -> bool:
    """Open a raw TCP socket to the host on port 80."""
    try:
        with closing(socket.create_connection((TEST_HOST, 80), timeout=TIMEOUT)):
            return True
    except Exception as e:
        # print(f"Socket check failed: {e}", file=sys.stderr)
        return False


def main() -> None:
    results = {
        "http": http_check(),
        "dns":  dns_check(),
        "socket": socket_check(),
    }

    # Print a concise human‑readable report
    print("Internet connectivity test results:")
    for method, ok in results.items():
        print(f"  {method:6}: {'✅ OK' if ok else '❌ FAILED'}")

    # Overall verdict – at least one method succeeded
    if any(results.values()):
        print("\n✅ This process can reach the Internet.")
    else:
        print("\n❌ No outbound connectivity detected.")
        print("    • Verify outbound firewalls, proxy settings, or VPC rules.")
        print("    • Some hosted providers block port 80/443 by default.")


if __name__ == "__main__":
    main()
