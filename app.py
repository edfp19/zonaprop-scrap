"""Vercel entrypoint; the static dashboard is served separately by its CDN."""

from zona_oportunidades.presentation.api.main import app as app
