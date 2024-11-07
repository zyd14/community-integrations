# Dagster resource for Not Diamond ¬◇

Not Diamond is an AI model router that automatically determines which LLM is best-suited to respond to any query, improving LLM output quality by combining multiple LLMs into a **meta-model** that learns when to call each LLM.

## Installation

```bash
pip install dagster-contrib-notdiamond
```

## Configuration

Make sure you have followed the steps in the [Quickstart docs][quickstart], and added a Not Diamond API key to your environment.

## Usage

Define a resource for Not Diamond, then create the asset or ops:

```python
from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    GraphDefinition,
    OpExecutionContext,
    asset,
    define_asset_job,
    op,
)
from dagster_contrib_notdiamond import NotDiamondResource

@op
def notdiamond_op(context: OpExecutionContext, notdiamond: NotDiamondResource):
    with notdiamond.get_client(context) as client:
        session_id, best_llm = client.model_select(
            models=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
    return session_id, str(best_llm)

notdiamond_op_job = GraphDefinition(name="notdiamond_op_job", node_defs=[notdiamond_op]).to_job()

# Or if creating an asset:
@asset(compute_kind="NotDiamond")
def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> Tuple[str, str]:
    with notdiamond.get_client(context) as client:
        session_id, best_llm = client.model_select(
            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
    return session_id, str(best_llm)

notdiamond_asset_job = define_asset_job(name="notdiamond_asset_job", selection="notdiamond_asset")

defs = Definitions(
    assets=[notdiamond_asset],
    jobs=[notdiamond_asset_job, notdiamond_op_job],
    resources={
        "notdiamond": NotDiamondResource(api_key=EnvVar("NOTDIAMOND_API_KEY")),
    },
)
```

Please refer to `example_job/example_notdiamond.py` for an example script.

## Support

Visit the [documentation for Not Diamond][docs] or send a message to [support@notdiamond.ai][mailto]

[docs]: https://docs.notdiamond.ai
[mailto]: mailto:support@notdiamond.ai
[quickstart]: https://docs.notdiamond.ai/docs/quickstart
