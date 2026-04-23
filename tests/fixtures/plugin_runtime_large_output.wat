(module
  (memory (export "memory") 3)
  (global $input_cursor (mut i32) (i32.const 8192))

  (func (export "alloc_input") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $input_cursor))
    (global.set $input_cursor (i32.add (local.get $ptr) (local.get $len)))
    (local.get $ptr))

  (data (i32.const 4096)
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

  (func (export "run_tool") (param i32 i32) (result i64)
    (i64.or
      (i64.extend_i32_u (i32.const 4096))
      (i64.shl (i64.extend_i32_u (i32.const 256)) (i64.const 32))))
)
