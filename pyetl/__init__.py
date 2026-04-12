"""
pyetl — A metadata-driven ETL framework for building data warehouses.

Quick start:
    from pyetl import PyETLEngine
    engine = PyETLEngine()
    engine.run_full()

Or use individual components:
    from pyetl import PyETLRegistryReader, Extractor, Transformer, Loader
"""

from pyetl.engine      import PyETLEngine, RunResult
from pyetl.registry    import PyETLRegistryReader, TableConfig, ColumnConfig, LoadConfig, QualityRule
from pyetl.extractor   import Extractor
from pyetl.transformer import Transformer
from pyetl.loader      import Loader
from pyetl.quality     import QualityChecker

__version__ = "0.1.0"
__author__  = "junaidpk11"

__all__ = [
    "PyETLEngine",
    "RunResult",
    "PyETLRegistryReader",
    "TableConfig",
    "ColumnConfig",
    "LoadConfig",
    "QualityRule",
    "Extractor",
    "Transformer",
    "Loader",
    "QualityChecker",
]