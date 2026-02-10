export type Version = 3 | 4 | 5 | 2010;

export interface Dico<T> {
  [i: number]: T | undefined;
  [i: string]: T | undefined;
}

export type AccessParserLogLevel = "silent" | "error" | "warn" | "info";

export type AccessParserWarningCode =
  | "WARN_DB_OVERFLOW_FLAG_MISSING"
  | "WARN_DB_RECORD_METADATA_MISMATCH"
  | "WARN_DB_MEMO_LVAL2_UNSUPPORTED"
  | "WARN_DB_MEMO_FALLBACK_TO_BYTES";

export interface AccessParserWarning {
  code: AccessParserWarningCode;
  message: string;
  meta?: unknown;
  occurrence: number;
  signature: string;
}

export interface AccessParserError {
  code: string;
  message: string;
  error?: unknown;
  meta?: unknown;
}

export interface AccessParserOptions {
  logLevel?: AccessParserLogLevel;
  onWarning?: (warning: AccessParserWarning) => void;
  onError?: (error: AccessParserError) => void;
}
