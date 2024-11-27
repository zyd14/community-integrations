import shutil

import dagster as dg


@dg.asset(
    group_name="pipes",
    kinds={"rust"},
)
def example_rust_subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult:
    """Demonstrates running Rust binary in a subprocess."""
    cmd = [shutil.which("cargo"), "run"]
    cwd = dg.file_relative_path(__file__, "../rust_processing_jobs")
    return pipes_subprocess_client.run(
        command=cmd,
        cwd=cwd,
        context=context,
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[example_rust_subprocess_asset],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(
            context_injector=dg.PipesEnvContextInjector(),
        )
    },
)
