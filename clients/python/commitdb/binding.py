"""
Python ctypes bindings for libcommitdb shared library.
"""

import ctypes
import json
import os
import platform
from pathlib import Path
from typing import Optional


def _find_library() -> Optional[str]:
    """Find the libcommitdb shared library."""
    system = platform.system()
    
    if system == 'Darwin':
        lib_names = ['libcommitdb.dylib', 'libcommitdb-darwin-arm64.dylib', 'libcommitdb-darwin-amd64.dylib']
    elif system == 'Linux':
        lib_names = ['libcommitdb.so', 'libcommitdb-linux-amd64.so', 'libcommitdb-linux-arm64.so']
    elif system == 'Windows':
        lib_names = ['libcommitdb.dll']
    else:
        lib_names = ['libcommitdb.so', 'libcommitdb.dylib']
    
    # Search paths
    search_paths = [
        Path(__file__).parent / 'lib',  # Package lib directory (for pip installed)
        Path(__file__).parent,  # Same directory as this file
        Path(__file__).parent.parent / 'lib',  # clients/python/lib
        Path(__file__).parent.parent.parent.parent / 'lib',  # CommitDB/lib
        Path.cwd() / 'lib',  # ./lib
        Path.cwd(),  # Current directory
    ]
    
    for path in search_paths:
        for lib_name in lib_names:
            lib_path = path / lib_name
            if lib_path.exists():
                return str(lib_path)
    
    return None


class CommitDBBinding:
    """Low-level ctypes bindings for libcommitdb."""
    
    _lib: Optional[ctypes.CDLL] = None
    _lib_path: Optional[str] = None
    
    @classmethod
    def load(cls, lib_path: Optional[str] = None) -> 'CommitDBBinding':
        """Load the shared library."""
        if lib_path is None:
            lib_path = _find_library()
        
        if lib_path is None:
            raise RuntimeError(
                "Could not find libcommitdb shared library. "
                "Build it with: make lib"
            )
        
        cls._lib_path = lib_path
        cls._lib = ctypes.CDLL(lib_path)
        
        # Define function signatures
        cls._lib.commitdb_open_memory.argtypes = []
        cls._lib.commitdb_open_memory.restype = ctypes.c_int
        
        cls._lib.commitdb_open_file.argtypes = [ctypes.c_char_p]
        cls._lib.commitdb_open_file.restype = ctypes.c_int
        
        cls._lib.commitdb_close.argtypes = [ctypes.c_int]
        cls._lib.commitdb_close.restype = None
        
        # Use c_void_p for the result pointer to avoid automatic conversion
        cls._lib.commitdb_execute.argtypes = [ctypes.c_int, ctypes.c_char_p]
        cls._lib.commitdb_execute.restype = ctypes.c_void_p
        
        cls._lib.commitdb_free.argtypes = [ctypes.c_void_p]
        cls._lib.commitdb_free.restype = None
        
        return cls()
    
    @classmethod
    def is_loaded(cls) -> bool:
        """Check if the library is loaded."""
        return cls._lib is not None
    
    @classmethod
    def open_memory(cls) -> int:
        """Open an in-memory database."""
        if not cls.is_loaded():
            cls.load()
        handle = cls._lib.commitdb_open_memory()
        if handle < 0:
            raise RuntimeError("Failed to open in-memory database")
        return handle
    
    @classmethod
    def open_file(cls, path: str) -> int:
        """Open a file-based database."""
        if not cls.is_loaded():
            cls.load()
        handle = cls._lib.commitdb_open_file(path.encode('utf-8'))
        if handle < 0:
            raise RuntimeError(f"Failed to open database at {path}")
        return handle
    
    @classmethod
    def close(cls, handle: int) -> None:
        """Close a database handle."""
        if cls.is_loaded():
            cls._lib.commitdb_close(handle)
    
    @classmethod
    def execute(cls, handle: int, query: str) -> dict:
        """Execute a query and return the JSON response."""
        if not cls.is_loaded():
            raise RuntimeError("Library not loaded")
        
        result_ptr = cls._lib.commitdb_execute(handle, query.encode('utf-8'))
        if result_ptr is None or result_ptr == 0:
            raise RuntimeError("Query execution failed")
        
        try:
            # Cast void pointer to char pointer and read the string
            result_str = ctypes.cast(result_ptr, ctypes.c_char_p).value
            if result_str is None:
                raise RuntimeError("Empty response from query")
            result_json = result_str.decode('utf-8')
            return json.loads(result_json)
        finally:
            # Free the allocated memory using the void pointer
            cls._lib.commitdb_free(result_ptr)
