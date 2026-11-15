"""Loader for the pybind11 extension built from ``cpp_engine``.

``hft_native`` is imported lazily via :meth:`__getattr__` so that importing
this package never triggers a circular import: modules such as ``factory`` and
``bridge`` import ``hft_native`` directly, and the package's ``__init__`` must
stay importable before the extension module itself is loaded.
"""
from __future__ import annotations

import importlib
from typing import Any


def __getattr__(name: str) -> Any:
    if name == "hft_native":
        return importlib.import_module(".hft_native", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["hft_native"]
