"""
dashboard/streamlit_patch.py — Centralized, idempotent compatibility shims and patches for Streamlit 1.61.1.

Provides:
1. Starlette >= 0.38 compatibility shim (restores DEFAULT_EXCLUDED_CONTENT_TYPES and IdentityResponder).
2. MemoryMediaFileStorage download filename patch:
   Streamlit 1.61.1's client-side DownloadButton.tsx sets `anchor.setAttribute("download", "")`.
   In Chromium, an empty download attribute causes the browser to ignore Content-Disposition
   headers and name the download after the URL path leaf. By appending the explicit user filename
   as the leaf of the media URL (/_stcore/media/<file_id>/<safe_filename>), Chromium downloads the
   file with its exact intended filename without modifying Streamlit client code or breaking any
   public APIs.
"""

import logging
import os
import sys
import urllib.parse

logger = logging.getLogger("dashboard.streamlit_patch")


def apply_starlette_gzip_shim() -> bool:
    """Applies compatibility shim for Starlette >= 0.38 with Streamlit <= 1.61."""
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
        return True
    except ImportError:
        return False
    except Exception as exc:
        logger.warning("Failed to apply Starlette gzip compatibility shim: %s", exc)
        return False


def apply_download_filename_patch() -> bool:
    """
    Patches Streamlit MemoryMediaFileStorage to preserve user filenames in download URLs.
    
    Idempotent and safe against multiple invocations across launcher and script reload cycles.
    Logs clear warnings if compatibility checks fail.
    """
    try:
        import streamlit.runtime.memory_media_file_storage as _mmfs
        from streamlit.runtime.media_file_storage import MediaFileKind as _MediaFileKind
    except ImportError as exc:
        logger.warning(
            "Streamlit media storage modules could not be imported; skipping download filename patch: %s",
            exc,
        )
        return False

    cls = _mmfs.MemoryMediaFileStorage

    # Verification of expected internal API surface
    if not hasattr(cls, "get_url") or not hasattr(cls, "get_file"):
        logger.warning(
            "MemoryMediaFileStorage is missing expected get_url or get_file methods. "
            "Internal Streamlit API may have changed; skipping patch."
        )
        return False

    # Idempotency check: only install once
    if hasattr(cls, "_original_get_url"):
        return True

    try:
        cls._original_get_url = cls.get_url
        cls._original_get_file = cls.get_file

        def _custom_get_url(self, file_id: str) -> str:
            media_file = self._files_by_id.get(file_id)
            if media_file and media_file.filename and media_file.kind == _MediaFileKind.DOWNLOADABLE:
                safe_name = urllib.parse.quote(os.path.basename(media_file.filename))
                return f"{self._media_endpoint}/{file_id}/{safe_name}"
            return self._original_get_url(file_id)

        def _custom_get_file(self, filename: str) -> _mmfs.MemoryFile:
            clean_name = filename.split("?")[0].replace("\\", "/")
            parts = [p for p in clean_name.split("/") if p and p not in ("_stcore", "media")]
            if parts:
                fid = os.path.splitext(parts[0])[0]
                if fid in self._files_by_id:
                    return self._files_by_id[fid]
            return self._original_get_file(filename)

        cls.get_url = _custom_get_url
        cls.get_file = _custom_get_file
        logger.debug("Successfully applied Streamlit download filename patch.")
        return True
    except Exception as exc:
        logger.warning("Unexpected error applying download filename patch: %s", exc)
        return False


def patch_all() -> None:
    """Applies all required Streamlit compatibility shims and storage patches."""
    apply_starlette_gzip_shim()
    apply_download_filename_patch()
