import sys
import types
import typing

if "typing.io" not in sys.modules:
    try:
        import typing.io
    except ImportError:
        typing_io = types.ModuleType("typing.io")
        typing_io.BinaryIO = typing.BinaryIO
        typing_io.TextIO = typing.TextIO
        sys.modules["typing.io"] = typing_io


