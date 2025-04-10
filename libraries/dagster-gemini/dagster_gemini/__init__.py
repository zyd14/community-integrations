from dagster._core.libraries import DagsterLibraryRegistry

from dagster_gemini.resource import GeminiResource as GeminiResource

__version__ = "0.0.3"

DagsterLibraryRegistry.register("dagster-gemini", __version__)
