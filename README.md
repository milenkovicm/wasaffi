# Datafusion WASM User Defined Functions

Very simplistic datafusion user defined functions written in WASM.
POC has been built on top of [Wasmedge](https://wasmedge.org) library.
Not terribly performant with lot of coping and serializing data.

It has been implemented to demonstrate DataFusion `FunctionFactory` functionality ([arrow-datafusion/pull#9333](https://github.com/apache/arrow-datafusion/pull/9333)) & `WASM UDF` ([arrow-datafusion/pull#9326](https://github.com/apache/arrow-datafusion/issues/9326)).

Other project in `FunctionFactory` series:

- [Torchfusion, Opinionated Torch Inference on DataFusion](https://github.com/milenkovicm/torchfusion)
- [LightGBM Inference on DataFusion](https://github.com/milenkovicm/lightfusion)
- [Apache Datafusion JVM User Defined Functions (UDF), integration nobody asked for 😀](https://github.com/milenkovicm/adhesive)

> [!NOTE]
>
> - It has not been envisaged as a actively maintained library.
> - ~~I might give it another show with WasmEdge Plug-ins~~

## Installation

In order to be able to compile project WasmEdge library [should be installed](https://wasmedge.org/docs/start/install).

or using brew:

```bash
brew install wasmedge
```

## Define Function

Define a rust function ([wasm_function](wasm_function/)) like:

```rust
// expose function f1 as external function
// add required bindgen, and required serialization/deserialization
wasm_udf::export_udf_function!(f1);

/// standard datafusion udf ... kind of 
/// should return ArrayRef or ArrowError (or any error implementing to_string)
fn f1(args: &[ArrayRef]) -> Result<ArrayRef,ArrowError> {
    let base = args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 0 failed");
    let exponent = args[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 1 failed");

    let array = base
        .iter()
        .zip(exponent.iter())
        .map(|(base, exponent)| match (base, exponent) {
            (Some(base), Some(exponent)) => Some(base.powf(exponent)),
            _ => None,
        })
        .collect::<Float64Array>();

    Ok(Arc::new(array))
}
```

which will be converted to `wasm` module with:

```bash
cd wasm_function
cargo build
```

An artifact should be available at `target/wasm32-unknown-unknown/debug/wasm_function.wasm`.

`export_udf_function!` macro should add WasmEdge bindings and peace of code which would do Arrow IPC serialization/deserialization. Arrow arrays are effectively copied across rust/wasm boundary.

This code currently handles happy day scenario, with basic exceptional cases covered.

## UDF Declaration

```rust
let sql = r#"
CREATE FUNCTION f1(DOUBLE, DOUBLE)
RETURNS DOUBLE
LANGUAGE WASM
AS 'wasm_function.wasm!f1'
"#;

ctx.sql(sql).await?.show().await?;

ctx.sql("select a, b, f1(a,b) from t").await?.show().await?;
```

[full example](wasmedge_factory/examples/wasaffi.rs)

should produce something similar to:

```text
+-----+-----+-------------------+
| a   | b   | f1(t.a,t.b)       |
+-----+-----+-------------------+
| 2.0 | 2.0 | 4.0               |
| 3.0 | 3.0 | 27.0              |
| 4.0 | 4.0 | 256.0             |
| 5.0 | 5.1 | 3670.684197150057 |
+-----+-----+-------------------+
```

Function is declared in format `wasm_function.wasm!f1`, where `wasm_function.wasm` represents module to load and `f1` a function to call.

At the moment, each function will create its own wasm VM, which may not be the optimal solution (but good enough for purpose of this POC). It should be simple enough to cache wasm module and reuse it across different functions.

## TODO

- [ ] ~~To be investigated if [WasmEdge Plug-ins](https://wasmedge.org/docs/start/wasmedge/extensions/plugins/) can be used
to avoid some data coping~~.
