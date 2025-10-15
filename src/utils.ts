import { Version, Dico } from "./types";

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

export const parseType = function (
  dataType: DataType,
  buffer: Uint8Array,
  length?: number,
  version: Version = 3,
) {
  let parsed: number | string = "";
  const dataView = new DataView(
    buffer.buffer,
    buffer.byteOffset,
    buffer.byteLength,
  );
  switch (dataType) {
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
    case DataType.Money:
      const low = dataView.getUint32(0, true);
      const high = dataView.getInt32(4, true);
      const combined = low + high * 0x100000000;
      parsed = combined / 10000;
      break;
    case DataType.DateTime:
      const daysSinceEpoch = dataView.getFloat64(0, true);
      const daysPassed = Math.floor(daysSinceEpoch);
      const hoursPassedDecimal = daysSinceEpoch % 1;
      const hours = Math.floor(hoursPassedDecimal * 24);
      const minutes = Math.floor(((hoursPassedDecimal * 24) % 1) * 60);
      const seconds = Math.floor(
        ((((hoursPassedDecimal * 24) % 1) * 60) % 1) * 60,
      );
      const date = new Date("1899-12-30T12:00:00Z");
      date.setDate(date.getDate() + daysPassed);
      date.setHours(hours, minutes, seconds);
      parsed = date.toISOString();
      break;
    case DataType.Binary:
      parsed = new TextDecoder("utf-8").decode(buffer.subarray(0, length));
      break;
    case DataType.GUID:
      const guidBytes = buffer.subarray(0, 16);
      parsed = uuidStringify(guidBytes);
      break;
    case DataType.Bit96Bytes17:
      parsed = new TextDecoder("utf-8").decode(buffer.subarray(0, 17));
      break;
    case DataType.Text:
      if (version > 3) {
        const first = buffer[0] === 0xfe && buffer[1] === 0xff;
        const second = buffer[0] === 0xff && buffer[1] === 0xfe;
        if (first || second) {
          parsed = new TextDecoder("windows-1252").decode(buffer.subarray(2));
        } else {
          parsed = new TextDecoder("utf-16le").decode(buffer);
        }
      } else {
        parsed = new TextDecoder("utf-8").decode(buffer);
      }
      break;
  }
  return parsed;
};

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

export const categorizePages = function (
  dbData: Uint8Array,
  pageSize: number,
): [Dico<Uint8Array>, Dico<Uint8Array>, Dico<Uint8Array>] {
  if (dbData.length % pageSize)
    throw new Error(
      `DB is not full or pageSize is wrong. pageSize: ${pageSize} dbData.length: ${dbData.length}`,
    );
  const pages: Dico<Uint8Array> = {};
  for (let i = 0; i < dbData.length; i += pageSize)
    pages[i] = dbData.slice(i, i + pageSize);
  const dataPages: Dico<Uint8Array> = {};
  const tableDefs: Dico<Uint8Array> = {};
  for (const page of Object.keys(pages)) {
    const comp1 =
      compareUint8Arrays(
        DATA_PAGE_MAGIC,
        pages[page]!.subarray(0, DATA_PAGE_MAGIC.length),
      ) === 0;
    const comp2 =
      compareUint8Arrays(
        TABLE_PAGE_MAGIC,
        pages[page]!.subarray(0, TABLE_PAGE_MAGIC.length),
      ) === 0;
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
