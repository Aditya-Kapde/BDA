#!/usr/bin/env python3
"""
scripts/run_dashboard.py — Launch the Streamlit Clickstream Analytics Dashboard.

Ensures environment compatibility (Starlette >= 0.38 with Streamlit <= 1.61) and invokes
Streamlit CLI directly with dashboard/app.py.

Usage:
  python scripts/run_dashboard.py [--server.port 8501] [-- --reports-dir reports/daily-2019-11-01]
"""

import os
import sys

# Ensure UTF-8 stdout/stderr on Windows to prevent cp1252 charmap encoding errors
if sys.platform.startswith("win"):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Backward compatibility shim for starlette >= 0.38 with streamlit <= 1.61
try:
    import starlette.middleware.gzip as _sm_gzip
    if not hasattr(_sm_gzip, "DEFAULT_EXCLUDED_CONTENT_TYPES"):
        _sm_gzip.DEFAULT_EXCLUDED_CONTENT_TYPES = frozenset()
    if not hasattr(_sm_gzip, "IdentityResponder"):
        class _IdentityResponder:
            def __init__(self, app):
                self.app = app
            async def __call__(self, scope, receive, send):
                await self.app(scope, receive, send)
        _sm_gzip.IdentityResponder = _IdentityResponder
except ImportError:
    pass

from streamlit.web import cli as stcli

if __name__ == "__main__":
    app_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard", "app.py"
    )
    sys.argv = ["streamlit", "run", app_path] + sys.argv[1:]
    sys.exit(stcli.main())
