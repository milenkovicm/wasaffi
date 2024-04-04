use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::exec_err,
    error::Result,
    execution::context::{FunctionFactory, RegisterFunction, SessionState},
    logical_expr::{CreateFunction, DefinitionStatement, ScalarUDF},
};

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

        let (module, method) = match &statement.params.as_ {
            Some(DefinitionStatement::SingleQuotedDef(path)) => Self::module_function(path)?,
            None => return exec_err!("wasm function not defined "),
            Some(f) => return exec_err!("wasm function incorrect {:?} ", f),
        };

        let f = crate::udf::WasmFunctionWrapper::new(module, method, argument_types, return_type)?;

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}

impl WasmFunctionFactory {
    fn module_function(s: &str) -> Result<(String, String)> {
        match s.split('!').collect::<Vec<&str>>()[..] {
            [module, method] if !module.is_empty() && !method.is_empty() => {
                Ok((module.to_string(), method.to_string()))
            }
            _ => exec_err!("bad module/method format"),
        }
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
        let (module, method) = WasmFunctionFactory::module_function("module!method").unwrap();
        assert_eq!("module", module);
        assert_eq!("method", method);

        assert!(WasmFunctionFactory::module_function("!method").is_err());
    }
    #[tokio::test]
    async fn e2e() -> datafusion::error::Result<()> {
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
    async fn e2e_release() -> datafusion::error::Result<()> {
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
        AS 'wasm_function.wasm!f1'
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
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder().is_test(true).try_init();
}
