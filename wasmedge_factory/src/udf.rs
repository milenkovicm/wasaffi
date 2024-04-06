use std::sync::Arc;

use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    common::exec_err,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
};
use wasm_udf::{from_ipc, pack_array_with_schema, to_ipc};
use wasmedge_sdk::dock::{Param, VmDock};

#[derive(Debug)]
pub(crate) struct WasmFunctionWrapper {
    name: String,
    wasm_method: String,
    //wasm_module: String,
    //argument_types: Vec<DataType>,
    argument_schema: SchemaRef,
    signature: Signature,
    return_type: DataType,
    vm: Arc<VmDock>,
}

// WASAFFI

impl WasmFunctionWrapper {
    pub(crate) fn new(
        vm: Arc<VmDock>,
        name: String,
        argument_types: Vec<DataType>,
        return_type: DataType,
    ) -> Result<Self> {
        let fields = argument_types
            .iter()
            .enumerate()
            .map(|(i, f)| Field::new(format!("c{}", i), f.clone(), false))
            .collect::<Vec<_>>();

        // we cache the schema
        // as it will be used for every message
        // passed between rust and wasm (not sure if we can avoid that)
        let argument_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            // prefix is not really needed but it looks cool :)
            wasm_method: format!("__wasm_udf_{}", name),
            name,
            signature: Signature::exact(argument_types, Volatility::Volatile),
            return_type,
            argument_schema,
            vm,
        })
    }
}

impl ScalarUDFImpl for WasmFunctionWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> Result<datafusion::arrow::datatypes::DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let batch = pack_array_with_schema(&arrays, self.argument_schema.clone());

        let payload = to_ipc(&batch.schema(), batch);
        let params = vec![Param::VecU8(&payload)];

        match self.vm.run_func(&self.wasm_method, params).unwrap() {
            Ok(mut res) => {
                // we should add errors to the protocol
                let response = res.pop().unwrap().downcast::<Vec<u8>>().unwrap();
                let a = from_ipc(&response);
                // aso we expect single column as the result
                let result = a.column(0);
                Ok(ColumnarValue::from(result.clone() as ArrayRef))
            }
            Err(err) => {
                exec_err!("wasm call error: {}", err)
            }
        }
    }
}
