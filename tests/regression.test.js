const assert = require("assert");
const {
  parseType,
  parseRelativeObjectMetadataStruct,
  readPrimaryKeyColumnIds,
  DataType,
} = require("../lib");
const { AccessTable, TableObject } = require("../lib/src/access-table");
const { readActiveTablePages } = require("../lib/src/usage-map");

function float64LE(value) {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, value, true);
  return new Uint8Array(buffer);
}

function numericBytes(value) {
  const bytes = new Uint8Array(17);
  bytes[0] = value < 0n ? 0x80 : 0;
  const magnitude = value < 0n ? -value : value;
  const view = new DataView(bytes.buffer);
  for (let word = 0; word < 4; word += 1) {
    view.setUint32(
      1 + 4 * word,
      Number((magnitude >> BigInt((3 - word) * 32)) & 0xffffffffn),
      true,
    );
  }
  return bytes;
}

function jet4DataPage(rows, flags = []) {
  const page = new Uint8Array(4096);
  const view = new DataView(page.buffer);
  view.setUint16(12, rows.length, true);
  let start = page.length;
  for (const [index, row] of rows.entries()) {
    start -= row.length;
    page.set(row, start);
    view.setUint16(14 + index * 2, start | (flags[index] ?? 0), true);
  }
  return page;
}

function longValueFixture() {
  const definition = new Uint8Array(4096);
  const def = new DataView(definition.buffer);
  def.setUint16(43, 1, true); // Variable columns
  def.setUint16(45, 1, true); // Columns
  def.setUint8(63, DataType.OLE);
  definition.set([2, 0, 65, 0], 88); // Name "A"

  function row(memoLength, pointer, inline = []) {
    const bytes = new Uint8Array(21 + inline.length);
    const view = new DataView(bytes.buffer);
    view.setUint16(0, 1, true); // Row column count
    view.setUint32(2, memoLength, true);
    view.setUint32(6, pointer, true);
    bytes.set(inline, 14);
    view.setUint16(bytes.length - 7, 14 + inline.length, true); // Terminal offset
    view.setUint16(bytes.length - 5, 2, true); // Column offset
    view.setUint16(bytes.length - 3, 1, true); // Variable column count
    bytes[bytes.length - 1] = 1; // Present
    return bytes;
  }

  const first = Uint8Array.of(1, 2, 0, 0, 0xff, 0); // points to page 2, row 1
  const last = Uint8Array.of(0, 0, 0, 0, 0x80, 9, 9);
  const overflow = jet4DataPage([first, last]);
  const table = new TableObject(0, definition);
  table.linkedPages = [
    jet4DataPage(
      [
        row(0x80000002, 0, [0x11, 0x22, 0xee]),
        row(0x40000002, 0x201),
        row(3, 0x200),
        Uint8Array.of(0, 0),
      ],
      [0, 0, 0, 0x8000],
    ),
  ];
  return new AccessTable(table, 4, 4096, { 8192: overflow }, {}, "silent");
}

// Both data pages claim the table as owner, but only page 4 is allocated to it.
// The map lives in row 1 to exercise the preceding row as the record boundary.
function activePageFixture(version, mapType) {
  const pageSize = version === 3 ? 2048 : 4096;
  const bytes = new Uint8Array(pageSize * 6);
  const view = new DataView(bytes.buffer);
  for (const page of [1, 3, 4]) {
    bytes[page * pageSize] = 1;
    bytes[page * pageSize + 1] = 1;
  }
  view.setUint32(3 * pageSize + 4, 2, true);
  view.setUint32(4 * pageSize + 4, 2, true);
  const countOffset = pageSize + (version === 3 ? 8 : 12);
  const mapLength = mapType === 0 ? 6 : 9;
  const mapStart = pageSize - 1 - mapLength;
  view.setUint16(countOffset, 2, true);
  view.setUint16(countOffset + 2, pageSize - 1, true);
  view.setUint16(countOffset + 4, mapStart, true);
  const mapOffset = pageSize + mapStart;
  bytes[mapOffset] = mapType;
  if (mapType === 0) {
    view.setUint32(mapOffset + 1, 3, true);
    bytes[mapOffset + 5] = 0b10;
  } else {
    view.setUint32(mapOffset + 1, 5, true);
    bytes[5 * pageSize] = 5;
    bytes[5 * pageSize + 4] = 0b10000;
  }
  return { bytes, view, pageSize, mapOffset };
}

function run() {
  const start = parseType(DataType.DateTime, float64LE(0));
  assert.strictEqual(start, "1899-12-30T00:00:00.000Z");

  const withTime = parseType(DataType.DateTime, float64LE(2.25));
  assert.strictEqual(withTime, "1900-01-01T06:00:00.000Z");
  assert.strictEqual(parseType(DataType.DateTime, float64LE(Infinity)), "(Invalid Date)");
  assert.strictEqual(parseType(DataType.DateTime, float64LE(3_000_000)), "(Invalid Date)");
  assert.strictEqual(parseType(DataType.Text, Uint8Array.of(0x63, 0xe9, 0, 0x80)), "cé\u0080");
  assert.strictEqual(
    parseType(DataType.Text, Uint8Array.of(0xff, 0xfe, 0xc4, 0, 0x22, 0x6f, 0, 0xe9), undefined, 4),
    "Ä漢é",
  );
  for (const [value, scale, expected] of [
    [-1234567890123456789000000123n, 8, "-12345678901234567890.00000123"],
    [1n, 8, "0.00000001"],
    [123n, 0, "123"],
  ]) {
    assert.strictEqual(
      parseType(DataType.Bit96Bytes17, numericBytes(value), undefined, 4, scale),
      expected,
    );
  }
  const relative = parseRelativeObjectMetadataStruct(
    Uint8Array.of(0, 1, 0x01, 0x35, 0x01, 0x81),
    0,
    4,
  );
  assert.deepStrictEqual(relative.variableLengthFieldOffsets, [309]);
  assert.strictEqual(relative.varLenCount, 385);
  const indexes = new Uint8Array(52 + 28 * 2);
  indexes.fill(0xff, 4, 34);
  const indexView = new DataView(indexes.buffer);
  indexView.setUint16(4, 10, true);
  indexView.setUint16(7, 2, true);
  indexes[52 + 28 + 4 + 19] = 1; // The second logical index is the primary key.
  assert.deepStrictEqual(readPrimaryKeyColumnIds(indexes, 2, 1, 4), [10, 2]);
  const values = longValueFixture().parse().A;
  assert.deepStrictEqual(values, [
    Uint8Array.of(0x11, 0x22),
    Uint8Array.of(0, 0),
    Uint8Array.of(0xff, 0, 0x80),
  ]);

  for (const version of [3, 4]) {
    for (const mapType of [0, 1]) {
      const { bytes, view, pageSize, mapOffset } = activePageFixture(version, mapType);
      const read = () => readActiveTablePages(bytes, pageSize, version, 2, 0x101);
      const pages = read();
      assert.strictEqual(pages.length, 1);
      assert.strictEqual(pages[0].byteOffset, 4 * pageSize);
      assert.throws(() => readActiveTablePages(bytes, pageSize, version, 2, 0), /pointer/);
      view.setUint32(4 * pageSize + 4, 7, true);
      assert.throws(read, /belongs to table 7/);
      view.setUint32(4 * pageSize + 4, 2, true);
      if (mapType === 0) {
        bytes[mapOffset + 5] = 0b10000000;
        assert.throws(read, /missing page 10/);
        bytes[mapOffset + 5] = 0;
      } else {
        bytes[5 * pageSize] = 1;
        assert.throws(read, /bitmap page 5 has invalid type/);
        bytes[5 * pageSize] = 5;
        view.setUint32(mapOffset + 1, 6, true);
        assert.throws(read, /missing page 6/);
        view.setUint32(mapOffset + 1, 0, true);
      }
      assert.deepStrictEqual(read(), []);
      bytes[mapOffset] = 2;
      assert.throws(read, /Unsupported usage map type 2/);
    }
  }

  console.log("regression tests passed");
}

run();
