use serde_json::{from_str, from_value, Map, Value};

use crate::PipesContextData;

pub trait PipesContextLoader {
    fn load_context(&self, params: Map<String, Value>) -> PipesContextData;
}

/// Context loader that loads context data from either a file or directly from the provided params.
///
/// The location of the context data is configured by the params received by the loader. If the params
/// include a key `path`, then the context data will be loaded from a file at the specified path. If
/// the params instead include a key `data`, then the corresponding value should be a dict
/// representing the context data.
/// Translation of https://github.com/dagster-io/dagster/blob/258d9ca0db7fcc16d167e55fee35b3cf3f125b2e/python_modules/dagster-pipes/dagster_pipes/__init__.py#L604-L630
#[derive(Debug, Default)]
pub struct PipesDefaultContextLoader;

impl PipesDefaultContextLoader {
    pub fn new() -> Self {
        Self
    }
}

impl PipesContextLoader for PipesDefaultContextLoader {
    fn load_context(&self, params: Map<String, Value>) -> PipesContextData {
        const FILE_PATH_KEY: &str = "path";
        const DIRECT_KEY: &str = "data";

        match (params.get(FILE_PATH_KEY), params.get(DIRECT_KEY)) {
            (Some(Value::String(string)), None) => from_str(string).unwrap(),
            (None, Some(Value::Object(map))) => from_value(Value::Object(map.clone())).unwrap(),
            _ => panic!("Invalid params provided"), // TODO: Have this function return a Result
        }
    }
}

// TODO: Unit tests
