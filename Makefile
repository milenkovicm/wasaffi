.PHONY: all rust wasm test clean install fmt clippy

all: rust wasm

rust:
	cargo build 

test: wasm
	cargo test

wasm:
	cd wasm_function && cargo build

test-wasm:
	cd wasm_function && cargo test --target wasm32-unknown-unknown

clean-wasm:
	cd wasm_function && cargo clean

clean: clean-wasm
	cargo clean

install:
	# add other files here
	cargo install wasm-bindgen-cli

fmt:
	cargo fmt --all --
	cd wasm_function &&  cargo fmt --all --

clippy:
	cargo clippy
	cd wasm_function && cargo clippy