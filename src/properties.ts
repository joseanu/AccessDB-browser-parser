import type { Version } from "./types";
import { DataType, parseType } from "./utils";

/** Column display formats stored in the MSysObjects.LvProp property map. */
export function readColumnFormats(bytes: Uint8Array, version: Version): Map<string, string> {
  const formats = new Map<string, string>();
  const magic = String.fromCharCode(...bytes.subarray(0, 4));
  if (magic !== "MR2\0" && magic !== "KKD\0") return formats;

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const names: string[] = [];
  const text = (start: number, end: number) =>
    parseType(DataType.Text, bytes.subarray(start, end), end - start, version);

  for (let start = 4; start + 6 <= bytes.length; ) {
    const size = view.getUint32(start, true);
    const type = view.getUint16(start + 4, true);
    const end = start + size;
    if (size < 6 || end > bytes.length) break;
    const body = start + 6;
    start = end;

    if (type === 0x80) {
      names.length = 0;
      for (let offset = body; offset + 2 <= end; ) {
        const length = view.getUint16(offset, true);
        offset += 2;
        if (offset + length > end) break;
        names.push(text(offset, offset + length));
        offset += length;
      }
    } else if (type === 1 && body + 6 <= end) {
      const nameBlockSize = view.getUint32(body, true);
      const nameLength = view.getUint16(body + 4, true);
      if (nameBlockSize < 6 + nameLength || body + nameBlockSize > end) continue;
      const column = text(body + 6, body + 6 + nameLength);
      for (let offset = body + nameBlockSize; offset + 8 <= end; ) {
        const entrySize = view.getUint16(offset, true);
        const dataType = view.getUint8(offset + 3);
        const nameIndex = view.getUint16(offset + 4, true);
        const dataSize = view.getUint16(offset + 6, true);
        if (entrySize < 8 + dataSize || offset + entrySize > end) break;
        if (dataType === DataType.Text && names[nameIndex] === "Format") {
          formats.set(column, text(offset + 8, offset + 8 + dataSize));
        }
        offset += entrySize;
      }
    }
  }
  return formats;
}

const CURRENCY_FORMAT = {
  USD: new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }),
  EUR: new Intl.NumberFormat("en-US", { style: "currency", currency: "EUR" }),
  Percent: new Intl.NumberFormat("en-US", {
    style: "percent",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
  "General Number": new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }),
  Fixed: new Intl.NumberFormat("en-US", {
    useGrouping: false,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
  Standard: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
};

export function formatCurrency(value: number, format?: string): number | string {
  if (!format) return value;
  if (format.startsWith("$")) return CURRENCY_FORMAT.USD.format(value);
  if (format.startsWith("€")) return CURRENCY_FORMAT.EUR.format(value);
  switch (format) {
    case "Percent":
    case "General Number":
    case "Fixed":
    case "Standard":
      return CURRENCY_FORMAT[format].format(value);
    case "Scientific":
      return value.toExponential(2).replace(/e([+-])(\d)$/, "e$10$2");
    default:
      return value;
  }
}
