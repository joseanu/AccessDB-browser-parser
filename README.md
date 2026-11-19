# AccessDB Browser Parser

## Description

This is a fork of the pure javascript Microsoft AccessDB files (.mdb, .accdb) parser `accessdb-parser`, adapted to work in modern web browsers.

This version removes Node.js dependencies like `Buffer` and uses `Uint8Array` and other Web APIs instead, making it suitable for client-side applications.

## Usage

```javascript
import { AccessParser } from "accessdb-parser";

async function loadAndParseDB() {
  const response = await fetch("path/to/your/database.mdb");
  const arrayBuffer = await response.arrayBuffer();
  const dbData = new Uint8Array(arrayBuffer);

  const db = new AccessParser(dbData);

  const tables = db.getTables(); // -> ["tableName1", "tableName2"]
  console.log("Tables:", tables);

  const table = db.parseTable("tableName1");
  console.log("Table Data:", table);
  // -> [{data: {name: "John", age: 23}, rowNumber: 1},{data: {name: "Bill", age: 56}, rowNumber: 2}]
}

loadAndParseDB();
```

Use `db.getPrimaryKeys(tableName)` to read the primary-key column names in index
order. Tables without a primary key return an empty array.

Numeric (Decimal) values are returned as exact strings at the column's declared
scale. OLE fields return `Uint8Array` bytes. Memo and OLE fields can be stored
inline, in one separate record, or across a chain of records. Dates use ISO
strings, with `"(Invalid Date)"` for out-of-range values.

Money fields return numbers by default. Pass `{ formatMoney: true }` as the second
argument to `AccessParser` to use supported column display formats from the
database. These include dollar, euro, Percent, General Number, Fixed, Standard,
and Scientific formats. A column with no recognized format still returns numbers.

## TypeScript

This project has types declaration.

## Active table pages

Tables are read from their allocation usage map, including the `MSysObjects`
catalog. A data page's owner header alone does not make that page part of a table:
released pages may retain old rows and the old owner value.

The reader supports inline (type 0) and indirect (type 1) usage maps for the page
sizes already supported by the parser. It checks map pointers, row boundaries,
bitmap page types, allocated page existence and table ownership. It never falls
back to scanning owner headers when a map is missing, malformed or unsupported.
Flagged map records and unknown map types are rejected explicitly.

A valid map with no allocated data pages returns an empty array. An invalid map
throws an error prefixed with `ERR_DB_USAGE_MAP_INVALID` and calls the existing
`onError` callback with that code and the table name, table definition page and
usage map pointer. Invalid catalog maps fail in the constructor; other table maps
are checked when `parseTable` reads that table. No public method signature changes.

## Todo

- unparse

## Special thanks

- The original [accessdb-parser](https://github.com/quentinjanuel/accessdb-parser)
- [Claroty's Python access_parser](https://github.com/claroty/access_parser), the
  reference for the Numeric, long-value, currency metadata, text and primary-key
  updates.
- https://github.com/brianb/mdbtools
