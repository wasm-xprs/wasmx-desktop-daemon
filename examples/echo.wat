(module
  (import "wasmx" "input_len" (func $input_len (result i32)))
  (import "wasmx" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "wasmx" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "wasmx_main") (result i32)
    (local $n i32)
    call $input_len
    local.set $n
    i32.const 0
    i32.const 0
    local.get $n
    call $input_read
    drop
    i32.const 0
    local.get $n
    call $output_write
    drop
    i32.const 0))
