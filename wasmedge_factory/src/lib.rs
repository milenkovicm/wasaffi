use std::{path::Path, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    common::exec_err,
    error::{DataFusionError, Result},
    execution::context::{FunctionFactory, RegisterFunction, SessionState},
    logical_expr::{CreateFunction, DefinitionStatement, ScalarUDF},
};
use thiserror::Error;
use wasmedge_sdk::{config::ConfigBuilder, dock::VmDock, Module, VmBuilder};

mod udf;
#[derive(Default)]
pub struct WasmFunctionFactory {}

#[async_trait::async_trait]
impl FunctionFactory for WasmFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let return_type = statement.return_type.expect("return type expected");
        let argument_types = statement
            .args
            .map(|args| {
                args.into_iter()
                    .map(|a| a.data_type)
                    .collect::<Vec<DataType>>()
            })
            .unwrap_or_default();
        let declared_name = statement.name;
        let (module_name, method_name) = match &statement.params.as_ {
            Some(DefinitionStatement::SingleQuotedDef(path)) => Self::wasm_module_function(path)?,
            None => return exec_err!("wasm function not defined "),
            Some(f) => return exec_err!("wasm function incorrect {:?} ", f),
        };

        // we could have have vm and module cached in FunctionFactory
        // and reuse across the functions if needed
        let vm = WasmFunctionFactory::wasm_model_load(&module_name)?;
        let f = crate::udf::WasmFunctionWrapper::new(
            vm,
            declared_name,
            method_name,
            argument_types,
            return_type,
        )?;

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}

impl WasmFunctionFactory {
    fn wasm_module_function(s: &str) -> Result<(String, String)> {
        match s.split('!').collect::<Vec<&str>>()[..] {
            [module, method] if !module.is_empty() && !method.is_empty() => {
                Ok((module.to_string(), method.to_string()))
            }
            _ => exec_err!("bad module/method format"),
        }
    }

    fn wasm_model_load(wasm_module: &str) -> std::result::Result<Arc<VmDock>, WasmFunctionError> {
        let file = Path::new(&wasm_module);
        let module = if file.is_absolute() {
            Module::from_file(None, wasm_module)?
        } else {
            let mut project_root = project_root::get_project_root()
                .map_err(|e| WasmFunctionError::Execution(e.to_string()))?;
            project_root.push(file);
            Module::from_file(None, &project_root)?
        };

        // default configuration will do for now
        let config = ConfigBuilder::default().build()?;

        let vm = VmBuilder::new()
            .with_config(config)
            .build()?
            .register_module(None, module)?;

        Ok(Arc::new(VmDock::new(vm)))
    }
}

#[derive(Error, Debug)]
pub enum WasmFunctionError {
    #[error("WasmEdge Error: {0}")]
    WasmEdgeError(#[from] Box<wasmedge_sdk::error::WasmEdgeError>),
    #[error("Execution Error: {0}")]
    Execution(String),
}

impl From<WasmFunctionError> for DataFusionError {
    fn from(e: WasmFunctionError) -> Self {
        // will do for now
        DataFusionError::Execution(e.to_string())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        arrow::array::{ArrayRef, Float64Array, RecordBatch},
        assert_batches_eq,
        execution::context::SessionContext,
    };

    use crate::WasmFunctionFactory;

    #[test]
    fn test_module_function_split() {
        let (module, method) = WasmFunctionFactory::wasm_module_function("module!method").unwrap();
        assert_eq!("module", module);
        assert_eq!("method", method);

        assert!(WasmFunctionFactory::wasm_module_function("!method").is_err());
    }
    #[tokio::test]
    async fn should_handle_happy_path() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let a: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 3.0, 4.0, 5.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 3.0, 4.0, 5.1]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

        ctx.register_batch("t", batch)?;

        let sql = r#"
        CREATE FUNCTION f1(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f1'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx
            .sql("select a, b, f1(a,b) from t")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+-----+-----+-------------------+",
            "| a   | b   | f1(t.a,t.b)       |",
            "+-----+-----+-------------------+",
            "| 2.0 | 2.0 | 4.0               |",
            "| 3.0 | 3.0 | 27.0              |",
            "| 4.0 | 4.0 | 256.0             |",
            "| 5.0 | 5.1 | 3670.684197150057 |",
            "+-----+-----+-------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_handle_error() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f2(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_return_error'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f2(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation] wasm function returned error",
            result.err().unwrap().to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_handle_arrow_error() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f2(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_return_arrow_error'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f2(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation] Divide by zero error",
            result.err().unwrap().to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_handle_panic() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f1(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f1'
        "#;
        // we register good function to verify that panich
        // will not put vm to some unexpected state
        ctx.sql(sql).await?.show().await?;

        let sql = r#"
        CREATE FUNCTION f3(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_panic'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f3(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation Panic] unreachable",
            result.err().unwrap().to_string()
        );
        let result = ctx.sql("select f1(1.0,1.0)").await?.collect().await?;
        let expected = vec![
            "+---------------------------+",
            "| f1(Float64(1),Float64(1)) |",
            "+---------------------------+",
            "| 1.0                       |",
            "+---------------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder().is_test(true).try_init();
}
