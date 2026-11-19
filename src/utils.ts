import type { Dico, Version } from "./types";

export enum DataType {
  Boolean = 1,
  Int8 = 2,
  Int16 = 3,
  Int32 = 4,
  Money = 5,
  Float32 = 6,
  Float64 = 7,
  DateTime = 8,
  Binary = 9,
  Text = 10,
  OLE = 11,
  Memo = 12,
  GUID = 15,
  Bit96Bytes17 = 16,
  Complex = 18,
}

const TABLE_PAGE_MAGIC = new Uint8Array([0x02, 0x01]);
const DATA_PAGE_MAGIC = new Uint8Array([0x01, 0x01]);
const ACCESS_EPOCH = Date.UTC(1899, 11, 30);
const MILLISECONDS_PER_DAY = 86_400_000;
const UTF8 = new TextDecoder("utf-8", { fatal: true });
const UTF16LE = new TextDecoder("utf-16le");
const UTF16BE = new TextDecoder("utf-16be");

function decodeLegacyText(bytes: Uint8Array): string {
  try {
    return UTF8.decode(bytes);
  } catch {
    // The browser's "latin1" decoder actually uses Windows-1252.
    return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
  }
}

function decodeAccessText(bytes: Uint8Array, version: Version): string {
  if (version === 3) return decodeLegacyText(bytes);
  if (bytes[0] === 0xfe && bytes[1] === 0xff) return UTF16BE.decode(bytes);
  if (bytes[0] !== 0xff || bytes[1] !== 0xfe) return UTF16LE.decode(bytes);

  // A zero byte switches between compressed and uncompressed segments.
  const expanded = new Uint8Array((bytes.length - 2) * 2);
  let compressed = true;
  let length = 0;
  for (const byte of bytes.subarray(2)) {
    if (byte === 0) {
      compressed = !compressed;
      continue;
    }
    expanded[length++] = byte;
    if (compressed) expanded[length++] = 0;
  }
  return UTF16LE.decode(expanded.subarray(0, length));
}

function decodeNumeric(bytes: Uint8Array, scale: number): string {
  if (bytes.length !== 17) throw new RangeError("Access Numeric values require 17 bytes");
  if (!Number.isInteger(scale) || scale < 0 || scale > 28) {
    throw new RangeError("Invalid Access Numeric scale");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let magnitude = BigInt(0);
  // Each 32-bit word is little-endian, in descending significance.
  for (let offset = 1; offset < 17; offset += 4) {
    magnitude = (magnitude << BigInt(32)) + BigInt(view.getUint32(offset, true));
  }
  const digits = magnitude.toString().padStart(scale + 1, "0");
  const sign = bytes[0] && magnitude !== BigInt(0) ? "-" : "";
  return scale === 0 ? sign + digits : sign + digits.slice(0, -scale) + "." + digits.slice(-scale);
}

export function parseType(
  dataType: DataType.OLE,
  buffer: Uint8Array,
  length?: number,
  version?: Version,
): Uint8Array;
export function parseType(
  dataType: DataType.Text,
  buffer: Uint8Array,
  length?: number,
  version?: Version,
): string;
export function parseType(
  dataType: Exclude<DataType, DataType.OLE>,
  buffer: Uint8Array,
  length?: number,
  version?: Version,
  numericScale?: number,
): string | number | boolean;
export function parseType(
  dataType: DataType,
  buffer: Uint8Array,
  length?: number,
  version?: Version,
  numericScale?: number,
): string | number | boolean | Uint8Array;
export function parseType(
  dataType: DataType,
  buffer: Uint8Array,
  length?: number,
  version: Version = 3,
  numericScale = 6,
): string | number | boolean | Uint8Array {
  let parsed: number | string | boolean;
  const dataView = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  switch (dataType) {
    case DataType.Boolean:
      parsed = buffer[0] !== 0;
      break;
    case DataType.Int8:
      parsed = dataView.getInt8(0);
      break;
    case DataType.Int16:
      parsed = dataView.getInt16(0, true);
      break;
    case DataType.Int32:
    case DataType.Complex:
      parsed = dataView.getInt32(0, true);
      break;
    case DataType.Float32:
      parsed = dataView.getFloat32(0, true);
      break;
    case DataType.Float64:
      parsed = dataView.getFloat64(0, true);
      break;
    case DataType.Money: {
      const low = dataView.getUint32(0, true);
      const high = dataView.getInt32(4, true);
      const combined = low + high * 0x100000000;
      parsed = combined / 10000;
      break;
    }
    case DataType.DateTime: {
      const daysSinceEpoch = dataView.getFloat64(0, true);
      const date = new Date(ACCESS_EPOCH + daysSinceEpoch * MILLISECONDS_PER_DAY);
      const year = date.getUTCFullYear();
      parsed = year >= 1 && year <= 9999 ? date.toISOString() : "(Invalid Date)";
      break;
    }
    case DataType.Binary:
      parsed = new TextDecoder("utf-8").decode(buffer.subarray(0, length));
      break;
    case DataType.OLE:
      return new Uint8Array(buffer.subarray(0, length));
    case DataType.GUID: {
      const guidBytes = buffer.subarray(0, 16);
      parsed = uuidStringify(guidBytes);
      break;
    }
    case DataType.Bit96Bytes17:
      parsed = decodeNumeric(buffer.subarray(0, length), numericScale);
      break;
    case DataType.Text:
      parsed = decodeAccessText(buffer, version).replace(/\u0000/g, "");
      break;
    default:
      parsed = new TextDecoder("utf-8").decode(buffer.subarray(0, length ?? buffer.length));
      break;
  }
  return parsed;
}

function uuidStringify(bytes: Uint8Array): string {
  const hex = Array.from(bytes).map((b) => ("00" + b.toString(16)).slice(-2));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}

export const categorizePages = (
  dbData: Uint8Array,
  pageSize: number,
): [Dico<Uint8Array>, Dico<Uint8Array>, Dico<Uint8Array>] => {
  if (dbData.length % pageSize)
    throw new Error(
      `DB is not full or pageSize is wrong. pageSize: ${pageSize} dbData.length: ${dbData.length}`,
    );
  const pages: Dico<Uint8Array> = {};
  for (let i = 0; i < dbData.length; i += pageSize) pages[i] = dbData.slice(i, i + pageSize);
  const dataPages: Dico<Uint8Array> = {};
  const tableDefs: Dico<Uint8Array> = {};
  for (const page of Object.keys(pages)) {
    const comp1 =
      compareUint8Arrays(DATA_PAGE_MAGIC, pages[page]!.subarray(0, DATA_PAGE_MAGIC.length)) === 0;
    const comp2 =
      compareUint8Arrays(TABLE_PAGE_MAGIC, pages[page]!.subarray(0, TABLE_PAGE_MAGIC.length)) === 0;
    if (comp1) dataPages[page] = pages[page];
    else if (comp2) tableDefs[page] = pages[page];
  }
  return [tableDefs, dataPages, pages];
};

function compareUint8Arrays(a: Uint8Array, b: Uint8Array): number {
  if (a.length !== b.length) {
    return a.length - b.length;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return a[i] - b[i];
    }
  }
  return 0;
}
