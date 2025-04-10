from dagster._core.libraries import DagsterLibraryRegistry

__version__ = "0.1.6"

DagsterLibraryRegistry.register("dagster-hightouch", __version__)
