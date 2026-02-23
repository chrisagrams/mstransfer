"""mstransfer — Transfer mass spectrometry files between endpoints."""

__version__ = "0.1.0"

from mstransfer.server.app import create_app

__all__ = ["__version__", "create_app"]
