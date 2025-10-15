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

## TypeScript

This project has types declaration.

## Todo

- unparse

## Special thanks

- The original [accessdb-parser](https://github.com/quentinjanuel/accessdb-parser)
- https://github.com/ClarotyICS/access_parser
- https://github.com/brianb/mdbtools