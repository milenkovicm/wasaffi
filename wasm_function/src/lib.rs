use arrow::array::{Array, ArrayRef, Float64Array};
use std::sync::Arc;
use wasm_udf::*;

// ```bash
// cargo install wasm-bindgen-cli
// ```

// ```bash
// cargo test --target wasm32-unknown-unknown
// ```

// expose function f1 as external function
// add required bindgen, and required serialization/deserialization
export_udf_function!(f1);

// standard datafusion udf ... kind of
fn f1(args: &[ArrayRef]) -> ArrayRef {
    assert_eq!(2, args.len());

    let base = args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 0 failed");
    let exponent = args[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 1 failed");

    assert_eq!(exponent.len(), base.len());

    let array = base
        .iter()
        .zip(exponent.iter())
        .map(|(base, exponent)| match (base, exponent) {
            (Some(base), Some(exponent)) => Some(base.powf(exponent)),
            _ => None,
        })
        .collect::<Float64Array>();

    Arc::new(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Float64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    use std::sync::Arc;

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn test_f1() {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![2.1, 3.1, 4.1, 5.1]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let args = vec![a, b];
        let result = f1(&args);

        assert_eq!(4, result.len())
    }
}
