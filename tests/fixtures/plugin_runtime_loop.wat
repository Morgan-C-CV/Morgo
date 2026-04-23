(module
  (memory (export "memory") 1)
  (global $input_cursor (mut i32) (i32.const 1024))

  (func (export "alloc_input") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $input_cursor))
    (global.set $input_cursor (i32.add (local.get $ptr) (local.get $len)))
    (local.get $ptr))

  (func (export "run_tool") (param i32 i32) (result i64)
    (loop $spin
      (br $spin))
    (i64.const 0))
)
