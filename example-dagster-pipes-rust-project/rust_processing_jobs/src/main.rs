use dagster_pipes_rust::open_dagster_pipes;
use serde_json::json;

fn check_large_dataframe_for_nulls() -> u32 {
    // smoke and mirrors
    return 1;
}

fn main() {
    let mut context = open_dagster_pipes();
    let null_count = check_large_dataframe_for_nulls();
    let passed = null_count == 0;
    let metadata = json!({"null_count": {"raw_value": null_count, "type": "int"}});
    context.report_asset_check(
        "telem_post_processing_check",
        passed,
        "telem_post_processing",
        metadata,
    );
}
