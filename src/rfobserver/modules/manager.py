"""Module manager — registry, lifecycle, and feed dispatch."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from rfobserver.modules.base import UpstreamModule  # noqa: TC001

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

# Module type registry — add new module types here
_REGISTRY: dict[str, type[UpstreamModule]] = {}


def register_module(cls: type[UpstreamModule]) -> type[UpstreamModule]:
    """Decorator to register a module type."""
    _REGISTRY[cls.module_type] = cls
    return cls


class ModuleManager:
    """Manages upstream signal processing modules."""

    def __init__(self) -> None:
        self._modules: dict[str, UpstreamModule] = {}

    @staticmethod
    def available_types() -> list[str]:
        return list(_REGISTRY.keys())

    @staticmethod
    def registry_info() -> dict[str, dict[str, Any]]:
        """Return parameter descriptors and capabilities for each module type."""
        from dataclasses import asdict

        info: dict[str, dict[str, Any]] = {}
        for name, cls in _REGISTRY.items():
            info[name] = {
                "parameters": [asdict(p) for p in cls.parameters()],
                "has_audio": cls.has_audio_output,
            }
        return info

    def create_module(
        self, module_type: str, params: dict[str, Any] | None = None
    ) -> UpstreamModule:
        """Create and start a new module instance."""
        if module_type not in _REGISTRY:
            msg = f"Unknown module type: {module_type}. Available: {list(_REGISTRY.keys())}"
            raise ValueError(msg)

        cls = _REGISTRY[module_type]
        module = cls(params)
        module.start()
        self._modules[module.module_id] = module
        logger.info(
            "Module created: %s (type=%s, id=%s)",
            cls.__name__,
            module_type,
            module.module_id,
        )
        return module

    def remove_module(self, module_id: str) -> None:
        """Stop and remove a module."""
        module = self._modules.pop(module_id, None)
        if module is not None:
            module.stop()
            logger.info("Module removed: %s", module_id)

    def get_module(self, module_id: str) -> UpstreamModule | None:
        return self._modules.get(module_id)

    def list_modules(self) -> list[dict[str, Any]]:
        return [m.status() for m in self._modules.values()]

    def feed_all(
        self,
        sc16_buf: np.ndarray,
        center_freq_hz: int,
        sample_rate: int,
    ) -> None:
        """Feed an IQ chunk to all active modules. Non-blocking."""
        for module in list(self._modules.values()):
            try:
                module.feed(sc16_buf, center_freq_hz, sample_rate)
            except Exception:
                logger.exception("Module %s feed error", module.module_id)

    def stop_all(self) -> None:
        """Stop all modules."""
        for module_id in list(self._modules):
            self.remove_module(module_id)
