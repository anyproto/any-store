
## AnyEnc Binary Format

**AnyEnc** is a high-performance binary format used by **Any-Store**. The format is designed to handle various data types, including raw binary, and allows direct byte-level comparisons via `bytes.Compare`. Its structure is similar to JSON. The Go library’s interface closely resembles that of `fastjson`, providing a high-performance, memory-efficient API.

### Data Format Structure

Each serialized item starts with a byte that defines its type. Supported data types and their corresponding byte identifiers:

- **0x01** — Null value
- **0x02** — Number (float64)
- **0x03** — String
- **0x04** — Boolean False
- **0x05** — Boolean True
- **0x06** — Array
- **0x07** — Object
- **0x08** — Binary data
- **0x09** — Compressed object (S2)
- **0x0A** — Vector (packed little-endian float32)
- **0x0B** — ObjectID (12-byte identifier)
- **0x0C** — DateTime (Unix-millisecond timestamp)

#### Serialization Details:

- **Null, True, False**: Represented by a single byte indicating the type (`0x01`, `0x05`, or `0x04`).
- **Number**: A type byte (`0x02`) followed by an 8-byte encoded number.
- **String**: A type byte (`0x03`), followed by the string data, terminated by `0x00`.
- **Array**: A type byte (`0x06`), followed by serialized elements, ending with `0x00`.
- **Object**: A type byte (`0x07`), followed by key-value pairs (key string terminated by `0x00`, followed by the value), and ending with `0x00` for both key and value.
  The special case applied for empty keys - the empty string in the key is replaced with the special byte `0x1F`
- **Binary**: A type byte (`0x08`), followed by a 4-byte big-endian uint32 length, and the binary data.
- **ObjectID**: A type byte (`0x0B`) followed by exactly 12 raw bytes — a fixed 13-byte encoding with *no* length prefix. The 12 bytes are big-endian (4-byte Unix timestamp, 5 process-unique bytes, 3-byte counter), so the encoding is directly `bytes.Compare`-orderable and therefore time-sortable, and appears in index keys (with an inverted form for reverse indexes).
- **DateTime**: A type byte (`0x0C`) followed by exactly 8 raw bytes — like ObjectID, a fixed-width encoding with *no* length prefix. The payload is the signed Unix-millisecond timestamp as a big-endian int64 with the sign bit flipped (offset-binary), so the encoding is directly `bytes.Compare`-orderable across the full signed range (pre-1970 included) and appears in index keys (inverted tag `0xF3` for reverse indexes).

### Example Encodings

- A string `"hello"`:  
  `0x03 + "hello" + 0x00`

- An array `[42, true]`:  
  `0x06 + 0x02 + <8-byte float for 42> + 0x05 + 0x00`

- An object `{"key": "value"}`:  
  `0x07 + "key" + 0x00 + 0x03 + "value" + 0x00 + 0x00`

- An object with empty key `{"":false}`:   
  `0x07 + 0x1F + 0x00 + 0x04 + 0x00`

- An ObjectID `0123456789abcdef01234567`:  
  `0x0B + 01 23 45 67 89 ab cd ef 01 23 45 67`
