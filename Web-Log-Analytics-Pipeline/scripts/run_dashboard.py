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

# Add project root to sys.path so dashboard package is importable
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Apply centralized shims and download filename patches
from dashboard import streamlit_patch
streamlit_patch.patch_all()

from streamlit.web import cli as stcli

if __name__ == "__main__":
    app_path = os.path.join(repo_root, "dashboard", "app.py")
    sys.argv = ["streamlit", "run", app_path] + sys.argv[1:]
    sys.exit(stcli.main())
