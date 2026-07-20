import sys
import types
import typing

# Enable typing to behave like a package for Python 3.13+ compatibility
if not hasattr(typing, "__path__"):
    typing.__path__ = []

try:
    import typing.io
except ImportError:
    typing_io = types.ModuleType("typing.io")
    typing_io.BinaryIO = typing.BinaryIO
    typing_io.TextIO = typing.TextIO
    sys.modules["typing.io"] = typing_io
    typing.io = typing_io
