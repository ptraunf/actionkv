# ActionKV 
Inspired by the ActionKV demo in chapter 7 of *Rust in Action* by Tim McNamara.
## Usage
```shell
akv_mem FILE get KEY
akv_mem FILE delete KEY
akv_mem FILE insert KEY VALUE
akv_mem FILE update KEY VALUE
```
## About
ActionKV is an implementation of Bitcask, a Log-Structured Hash Table
## Record Structure
Each KV pair is prefixed by 12 bytes which describe its length (key_len + value_len) and its content (checksum).
```
   Fixed-width Header           Variable-width  Variable-width Value
     │                             Key            │
 ┌───┴───────────────────────┐ ┌────┴──────┐ ┌────┴────────┐
 │checksum  key_len  val_len │ │   key     │ │    value    │
  0 1 2 3   4 5 6 7   8 9 A B
 └─┴─┴─┴─┘ └─┴─┴─┴─┘ └─┴─┴─┴─┘ └─┴─┴...┴─┴─┘ └─┴─┴...┴─┴─┴─┘
   u32       u32      u32      [u8; key_len] [u8; value_len]
```