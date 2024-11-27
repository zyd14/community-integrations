> [!WARNING]
> This project is under active development and is subject to change!

# Dagster Pipes Rust Implementation

Orchestrate your Rust workloads via Dagster through this light weight Rust interface.

## Usage

### Installation

```sh
cargo add dagster_pipes_rust
```

### Example

An example project can be found in [./example-dagster-pipes-rust-project](./example-dagster-pipes-rust-project).

In this project there exists a `rust_processing_jobs` binary, which demonstrates how to use the Dagster context to report materializations to Dagster through the `context.report_asset_materialization` method.

```rust
use dagster_pipes_rust::open_dagster_pipes;
use serde_json::json;

fn main() {
    let mut context = open_dagster_pipes();
    let metadata = json!({"row_count": {"raw_value": 100, "type": "int"}});
    context.report_asset_materialization("example_rust_subprocess_asset", metadata);
}
```

<img width="1355" alt="image" src="https://github.com/user-attachments/assets/ddc8c261-3e96-4e82-ad4c-723dd6b3dece">

It also demonstrates how to run the Rust binary in a subprocess from Dagster. Note, that it's also possible to launch processes in external compute environments like Kubernetes.

```python
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
```
