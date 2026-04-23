(module
  (memory (export "memory") 2)
  (global $input_cursor (mut i32) (i32.const 8192))

  (data (i32.const 1024) "STATIC-DATA-SHOULD-STAY-INTACT")
  (data (i32.const 4096) "static:")

  (func (export "alloc_input") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $input_cursor))
    (global.set $input_cursor (i32.add (local.get $ptr) (local.get $len)))
    (local.get $ptr))

  (func (export "run_tool") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $i i32)
    (local $out_ptr i32)
    (local $out_len i32)
    (local.set $out_ptr (i32.const 4096))
    (local.set $out_len (i32.add (i32.const 7) (local.get $in_len)))
    (local.set $i (i32.const 0))
    (block $done
      (loop $copy
        (br_if $done (i32.ge_u (local.get $i) (local.get $in_len)))
        (i32.store8
          (i32.add
            (local.get $out_ptr)
            (i32.add (i32.const 7) (local.get $i)))
          (i32.load8_u (i32.add (local.get $in_ptr) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $copy)))
    (i64.or
      (i64.extend_i32_u (local.get $out_ptr))
      (i64.shl (i64.extend_i32_u (local.get $out_len)) (i64.const 32))))
)
