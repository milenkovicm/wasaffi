use datafusion::{
    arrow::array::{ArrayRef, Float64Array, RecordBatch},
    execution::context::SessionContext,
};
use std::sync::Arc;
use wasedge_factory::WasmFunctionFactory;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

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

    ctx.sql("select a, b, f1(a,b) from t").await?.show().await?;

    Ok(())
}
