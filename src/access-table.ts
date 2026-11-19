import {
  MEMO,
  parseDataPageHeader,
  parseRelativeObjectMetadataStruct,
  parseTableData,
  parseTableHead,
  readPrimaryKeyColumnIds,
  TDEF_HEADER,
} from "./parsing-primitives";
import { formatCurrency } from "./properties";
import type {
  AccessParserError,
  AccessParserLogLevel,
  AccessParserWarning,
  AccessParserWarningCode,
  AccessValue,
  Dico,
  Version,
} from "./types";
import { DataType, parseType } from "./utils";

const LOG_LEVEL_RANK: { [K in AccessParserLogLevel]: number } = {
  silent: 0,
  error: 1,
  warn: 2,
  info: 3,
};
const WARN_PRINT_LIMIT = 3;

export class TableObject {
  public value: Uint8Array;
  public linkedPages: Array<Uint8Array> = [];
  public constructor(_offset: number, value: Uint8Array) {
    this.value = value;
    this.linkedPages = [];
  }
}

type PropType<TObj, TProp extends keyof TObj> = TObj[TProp];
type Column = PropType<ReturnType<typeof parseTableData>, "column">[0] & {
  colNameStr: string;
};
type TableHeader = ReturnType<typeof parseTableHead>;

export class AccessTable {
  private version: Version;
  private pageSize: number;
  private dataPages: Dico<Uint8Array>;
  private tableDefs: Dico<Uint8Array>;
  private table: TableObject;
  private columns: Column[];
  private tableHeader: TableHeader;
  private indexDefinitions: Uint8Array;
  private logLevel: AccessParserLogLevel;
  private onWarning?: (warning: AccessParserWarning) => void;
  private onError?: (error: AccessParserError) => void;
  private warningCountsBySignature: Map<string, number>;
  private warningTotalsByCode: Map<AccessParserWarningCode, number>;
  public constructor(
    table: TableObject,
    version: Version,
    pageSize: number,
    dataPages: Dico<Uint8Array>,
    tableDefs: Dico<Uint8Array>,
    logLevel: AccessParserLogLevel,
    onWarning?: (warning: AccessParserWarning) => void,
    onError?: (error: AccessParserError) => void,
    private moneyFormats?: ReadonlyMap<string, string>,
  ) {
    this.version = version;
    this.pageSize = pageSize;
    this.dataPages = dataPages;
    this.tableDefs = tableDefs;
    this.table = table;
    this.logLevel = logLevel;
    this.onWarning = onWarning;
    this.onError = onError;
    this.warningCountsBySignature = new Map<string, number>();
    this.warningTotalsByCode = new Map<AccessParserWarningCode, number>();
    [this.columns, this.tableHeader, this.indexDefinitions] = this.getTableColumns();
  }
  private shouldLog(level: "error" | "warn" | "info"): boolean {
    return LOG_LEVEL_RANK[this.logLevel] >= LOG_LEVEL_RANK[level];
  }
  private serializeMeta(meta: unknown): string {
    if (meta === undefined) return "";
    if (meta === null) return "null";
    if (typeof meta !== "object") return String(meta);
    if (Array.isArray(meta)) return `[${meta.map((item) => this.serializeMeta(item)).join(",")}]`;
    const obj = meta as { [key: string]: unknown };
    const keys = Object.keys(obj).sort();
    const entries = keys.map((key) => `${key}:${this.serializeMeta(obj[key])}`);
    return `{${entries.join(",")}}`;
  }
  private warn(code: AccessParserWarningCode, message: string, meta?: unknown): void {
    const signature = `${code}|${message}|${this.serializeMeta(meta)}`;
    const occurrence = (this.warningCountsBySignature.get(signature) ?? 0) + 1;
    this.warningCountsBySignature.set(signature, occurrence);
    this.warningTotalsByCode.set(code, (this.warningTotalsByCode.get(code) ?? 0) + 1);
    if (this.onWarning) this.onWarning({ code, message, meta, occurrence, signature });
    if (!this.shouldLog("warn")) return;
    if (occurrence > WARN_PRINT_LIMIT) return;
    if (meta !== undefined) console.warn(`[${code}] ${message}`, meta);
    else console.warn(`[${code}] ${message}`);
  }
  private error(code: string, message: string, error?: unknown, meta?: unknown): void {
    if (this.onError) this.onError({ code, message, error, meta });
    if (!this.shouldLog("error")) return;
    if (error !== undefined && meta !== undefined)
      console.error(`[${code}] ${message}`, { meta, error });
    else if (error !== undefined) console.error(`[${code}] ${message}`, error);
    else if (meta !== undefined) console.error(`[${code}] ${message}`, meta);
    else console.error(`[${code}] ${message}`);
  }
  private emitWarningsSummary(): void {
    if (!this.shouldLog("warn")) return;
    for (const [code, total] of this.warningTotalsByCode.entries()) {
      if (total <= WARN_PRINT_LIMIT) continue;
      console.warn(`[${code}] x${total}`);
    }
  }
  private getTableColumns(): [Column[], TableHeader, Uint8Array] {
    let tableHeader: TableHeader;
    let colNames: PropType<ReturnType<typeof parseTableData>, "columnNames">;
    let columns: Array<Column>;
    let indexDefinitions: Uint8Array;
    try {
      tableHeader = parseTableHead(this.table.value, this.version);
      let mergedData = this.table.value.slice(tableHeader.tDefHeaderEnd);
      if (tableHeader.TDEF_header.nextPagePtr) {
        mergedData = new Uint8Array([
          ...mergedData,
          ...this.mergeTableData(tableHeader.TDEF_header.nextPagePtr),
        ]);
      }
      const parsedData = parseTableData(
        mergedData,
        tableHeader.realIndexCount,
        tableHeader.columnCount,
        this.version,
      );
      columns = parsedData.column as any;
      colNames = parsedData.columnNames;
      indexDefinitions = mergedData.subarray(parsedData.columnMetadataEnd);
    } catch (err) {
      throw new Error(`Failed to parse table header`);
    }
    columns.forEach((c, i) => {
      c.colNameStr = colNames[i].colNameStr;
    });
    if (new Set(columns.map((column) => column.columnID)).size !== columns.length) {
      throw new Error("Duplicate column IDs in Access table definition");
    }
    const uniqueIndexes =
      new Set(columns.map((column) => column.columnIndex)).size === columns.length;
    columns.sort((a, b) =>
      uniqueIndexes ? a.columnIndex - b.columnIndex : a.columnID - b.columnID,
    );
    return [columns, tableHeader, indexDefinitions];
  }
  public getPrimaryKeys(): string[] {
    const ids = readPrimaryKeyColumnIds(
      this.indexDefinitions,
      this.tableHeader.indexCount,
      this.tableHeader.realIndexCount,
      this.version,
    );
    return ids.map((id) => {
      const column = this.columns.find((column) => column.columnID === id);
      if (!column) throw new Error(`Primary key references unknown Access column ${id}`);
      return column.colNameStr;
    });
  }
  private mergeTableData(firstPage: number): Uint8Array {
    let table = this.tableDefs[firstPage * this.pageSize]!;
    let parsedHeader = TDEF_HEADER.parse(table);
    let data = table.slice(parsedHeader.headerEnd);
    while (parsedHeader.nextPagePtr) {
      table = this.tableDefs[parsedHeader.nextPagePtr * this.pageSize]!;
      parsedHeader = TDEF_HEADER.parse(table);
      data = new Uint8Array([...data, ...table.slice(parsedHeader.headerEnd)]);
    }
    return data;
  }
  private getOverflowRecord(recordPointer: number, longValue = false): Uint8Array | undefined {
    const recordOffset = (recordPointer & 0xff) >>> 0;
    const pageNum = recordPointer >>> 8;
    const recordPage = this.dataPages[pageNum * this.pageSize];
    if (!recordPage) return;
    const parsedData = parseDataPageHeader(recordPage, this.version);
    if (recordOffset >= parsedData.recordOffsets.length) return;
    const recordOffsetMask = this.pageSize * 2 - 1;
    const rawStart = parsedData.recordOffsets[recordOffset];
    const start = rawStart & recordOffsetMask;
    const end =
      recordOffset === 0
        ? recordPage.length
        : parsedData.recordOffsets[recordOffset - 1] & recordOffsetMask;
    const directoryEnd = (this.version === 3 ? 10 : 14) + 2 * parsedData.recordOffsets.length;
    if (start < directoryEnd || start >= end || end > recordPage.length) return;
    if (!longValue && (rawStart & 0x8000) === 0) {
      this.warn("WARN_DB_OVERFLOW_FLAG_MISSING", "Overflow record flag is not present", {
        start,
        recordOffset,
        pageNum,
      });
    }
    return recordPage.subarray(start, end);
  }
  private readVariableOffsets(record: Uint8Array, bitmapStart: number): number[] {
    const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
    const jet3 = this.version === 3;
    const count = jet3 ? view.getUint8(bitmapStart - 1) : view.getUint16(bitmapStart - 2, true);
    if (count > this.tableHeader.variableColumns) {
      throw new Error(`Variable-length field count ${count} exceeds the table definition`);
    }

    const jumpSlots = jet3 ? Math.floor((record.length - 1) / 256) : 0;
    const width = jet3 ? 1 : 2;
    const offsetTableStart = bitmapStart - (count + 2) * width - jumpSlots;
    if (offsetTableStart < width) throw new Error("Variable-length metadata is outside the row");

    const offsets: number[] = [];
    if (jet3) {
      const reversed = new Uint8Array(record.subarray(offsetTableStart, bitmapStart)).reverse();
      const { variableLengthFieldOffsets, variableLengthJumpTable, varLenCount } =
        parseRelativeObjectMetadataStruct(reversed, jumpSlots, this.version);
      // A row can cross a 256-byte boundary entirely within its metadata.
      const jumpCount = Math.min(jumpSlots, Math.floor(offsetTableStart / 256));
      let jumps = 0;
      const lowOffsets = [...variableLengthFieldOffsets, varLenCount];
      for (const [index, lowOffset] of lowOffsets.entries()) {
        while (jumps < jumpCount && variableLengthJumpTable[jumps] === index) jumps += 1;
        offsets.push(lowOffset + jumps * 256);
      }
    } else {
      for (let index = 0; index <= count; index += 1) {
        offsets.push(view.getUint16(bitmapStart - 4 - 2 * index, true));
      }
    }

    let previous = width;
    for (const offset of offsets) {
      if (offset < previous || offset > offsetTableStart) {
        throw new Error("Variable-length field offset is outside the row data");
      }
      previous = offset;
    }
    return offsets;
  }

  private readLongValue(field: Uint8Array, column: string): Uint8Array {
    const { memoLength, recordPointer, memoEnd } = MEMO.parse(field);
    const length = memoLength & 0x3fffffff;
    const kind = memoLength >>> 30;
    if (kind === 2 || kind === 1) {
      const payload =
        kind === 2 ? field.subarray(memoEnd) : this.getOverflowRecord(recordPointer, true);
      if (!payload) throw new Error("Missing long-value record");
      if (payload.length < length) {
        this.warn(
          "WARN_DB_LONG_VALUE_TRUNCATED",
          "Long value is shorter than its declared length",
          {
            column,
            expected: length,
            actual: payload.length,
          },
        );
      }
      return new Uint8Array(payload.subarray(0, length));
    }
    if (kind !== 0) throw new Error("Invalid long-value flags");

    const chunks: Uint8Array[] = [];
    const visited = new Set<number>();
    let pointer = recordPointer;
    let total = 0;
    while (total < length) {
      if (pointer === 0 || visited.has(pointer)) {
        throw new Error("Invalid long-value chain");
      }
      visited.add(pointer);
      const record = this.getOverflowRecord(pointer, true);
      if (!record || record.length <= 4) throw new Error("Missing long-value chain record");
      const chunk = record.subarray(4, 4 + length - total);
      chunks.push(chunk);
      total += chunk.length;
      pointer = new DataView(record.buffer, record.byteOffset, record.byteLength).getUint32(
        0,
        true,
      );
    }
    const result = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result;
  }

  private parseField(column: Column, data: Uint8Array): AccessValue {
    if (column.type === DataType.Memo || column.type === DataType.OLE) {
      if (data.length === 0) return column.type === DataType.OLE ? new Uint8Array() : "";
      try {
        const payload = this.readLongValue(data, column.colNameStr);
        return column.type === DataType.OLE
          ? payload
          : parseType(DataType.Text, payload, payload.length, this.version);
      } catch {
        this.warn(
          "WARN_DB_MEMO_FALLBACK_TO_BYTES",
          "Failed to read long value. Using the stored field",
          {
            column: column.colNameStr,
          },
        );
        return column.type === DataType.OLE
          ? new Uint8Array(data)
          : new TextDecoder("utf-8").decode(data);
      }
    }
    if (data.length === 0 && !column.columnFlags.fixedLength) return "";
    if (column.type === DataType.Bit96Bytes17 && data.length !== 17) {
      this.warn("WARN_DB_NUMERIC_INVALID_LENGTH", "Invalid Numeric length. Using the stored bytes", {
        column: column.colNameStr,
        actual: data.length,
      });
      return new Uint8Array(data);
    }
    const value = parseType(column.type, data, data.length, this.version, column.various?.scale);
    if (column.type === DataType.Money && typeof value === "number") {
      return formatCurrency(value, this.moneyFormats?.get(column.colNameStr));
    }
    return value;
  }

  private parseRow(record: Uint8Array): AccessValue[] {
    const width = this.version === 3 ? 1 : 2;
    const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
    const columnCount = width === 1 ? view.getUint8(0) : view.getUint16(0, true);
    const bitmapStart = record.length - Math.ceil(columnCount / 8);
    if (bitmapStart < width) throw new Error("Null bitmap is outside the row");

    let offsets: number[] | undefined;
    return this.columns.map((column) => {
      const id = column.columnID;
      const hasValue =
        id < columnCount && (record[bitmapStart + (id >>> 3)] & (1 << (id & 7))) !== 0;
      if (column.type === DataType.Boolean) return hasValue;
      if (!hasValue) return null;

      if (column.columnFlags.fixedLength) {
        const start = width + column.fixedOffset;
        const end = start + column.length;
        if (end > bitmapStart)
          throw new Error(`Fixed-length field ${column.colNameStr} is outside the row`);
        return this.parseField(column, record.subarray(start, end));
      }

      offsets ??= this.readVariableOffsets(record, bitmapStart);
      const index = column.variableColumnNumber;
      if (index + 1 >= offsets.length) {
        throw new Error(`Variable-length field ${column.colNameStr} has no row offset`);
      }
      return this.parseField(column, record.subarray(offsets[index], offsets[index + 1]));
    });
  }

  public parse(): Dico<AccessValue[]> {
    const parsedTable: Dico<AccessValue[]> = Object.create(null);
    for (const column of this.columns) parsedTable[column.colNameStr] = [];
    const appendRow = (record: Uint8Array) => {
      try {
        const values = this.parseRow(record);
        for (const [index, column] of this.columns.entries()) {
          parsedTable[column.colNameStr]!.push(values[index]);
        }
      } catch (error) {
        this.warn("WARN_DB_ROW_SKIPPED_PARSE_ERROR", "Skipping row due parse error", {
          message: error instanceof Error ? error.message : String(error),
        });
      }
    };
    for (const dataChunk of this.table.linkedPages) {
      const originalData = dataChunk;
      const parsedData = parseDataPageHeader(originalData, this.version);
      const recordOffsetMask = this.pageSize * 2 - 1;
      let lastOffset: number | undefined;
      for (const recOffset of parsedData.recordOffsets) {
        if ((recOffset & 0x8000) >>> 0) {
          lastOffset = (recOffset & recordOffsetMask) >>> 0;
          continue;
        }
        if ((recOffset & 0x4000) >>> 0) {
          const recPtrOffset = (recOffset & recordOffsetMask) >>> 0;
          lastOffset = recPtrOffset;
          const overflowRecPtrBuffer = originalData.slice(recPtrOffset, recPtrOffset + 4);
          const dataView = new DataView(
            overflowRecPtrBuffer.buffer,
            overflowRecPtrBuffer.byteOffset,
            overflowRecPtrBuffer.byteLength,
          );
          const overflowRecPtr = dataView.getUint32(0, true);
          const record = this.getOverflowRecord(overflowRecPtr);
          if (record !== undefined) appendRow(record);
          continue;
        }
        let record: Uint8Array;
        if (!lastOffset) record = originalData.slice(recOffset);
        else record = originalData.slice(recOffset, lastOffset);
        lastOffset = recOffset;
        appendRow(record);
      }
    }
    this.emitWarningsSummary();
    return parsedTable;
  }
}
