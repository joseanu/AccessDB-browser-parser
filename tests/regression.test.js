const assert = require("assert");
const { parseType, DataType } = require("../lib");

function float64LE(value) {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, value, true);
  return new Uint8Array(buffer);
}

function run() {
  const start = parseType(DataType.DateTime, float64LE(0));
  assert.strictEqual(start, "1899-12-30T00:00:00.000Z");

  const withTime = parseType(DataType.DateTime, float64LE(2.25));
  assert.strictEqual(withTime, "1900-01-01T06:00:00.000Z");

  console.log("regression tests passed");
}

run();
