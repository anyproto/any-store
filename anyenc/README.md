
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

#### Serialization Details:

- **Null, True, False**: Represented by a single byte indicating the type (`0x01`, `0x05`, or `0x04`).
- **Number**: A type byte (`0x02`) followed by an 8-byte encoded numer.
- **String**: A type byte (`0x03`), followed by the string data, terminated by `0x00`.
- **Array**: A type byte (`0x06`), followed by serialized elements, ending with `0x00`.
- **Object**: A type byte (`0x07`), followed by key-value pairs (key string terminated by `0x00`, followed by the value), and ending with `0x00` for both key and value.
  The special case applied for empty keys - the empty string in the key is replaced with the special byte `0x1F`
- **Binary**: A type byte (`0x08`), followed by a 4-byte big-endian uint32 length, and the binary data.

### Example Encodings

- A string `"hello"`:  
  `0x03 + "hello" + 0x00`

- An array `[42, true]`:  
  `0x06 + 0x02 + <8-byte float for 42> + 0x05 + 0x00`

- An object `{"key": "value"}`:  
  `0x07 + "key" + 0x00 + 0x03 + "value" + 0x00 + 0x00`

- An object with empty key `{"":false}`:   
  `0x07 + 0x1F + 0x00 + 0x04 + 0x00`
