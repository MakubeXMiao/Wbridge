"""Optional backend drivers; importing the package never launches a provider."""

from .contract import BackendError

__all__ = ["BackendError"]
