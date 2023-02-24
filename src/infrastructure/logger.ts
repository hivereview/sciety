import axios from 'axios';
import rTracer from 'cls-rtracer';
import * as O from 'fp-ts/Option';
import { constant, flow, pipe } from 'fp-ts/function';
import { serializeError } from 'serialize-error';

enum Level {
  error,
  warn,
  info,
  debug,
}
type LevelName = keyof typeof Level;
export type Payload = Record<string, unknown>;

export type Logger = (level: LevelName, message: string, payload?: Payload, timestamp?: Date) => void;

export type LogEntry = { timestamp: Date, level: LevelName, message: string, payload?: Payload };

export const rTracerLogger = (logger: Logger): Logger => {
  const withRequestId = (payload: Payload) => pipe(
    O.of(rTracer.id()),
    O.fold(
      constant(payload),
      (requestId) => ({ ...payload, requestId }),
    ),
  );

  return (level, message, payload = {}) => (
    logger(level, message, withRequestId(payload))
  );
};

type Entry = {
  timestamp: Date,
  level: LevelName,
  message: string,
  payload: Payload,
};

type Serializer = (entry: Entry) => string;

export const replaceError = (_key: string, value: unknown): unknown => {
  if (_key === 'Authorization' || _key === 'Crossref-Plus-API-Token') {
    return '--redacted--';
  }
  if (value instanceof Error) {
    return serializeError(value);
  }
  return value;
};

const filterAxiosGarbageInPayload = (payload: Payload) => {
  if (payload.error && axios.isAxiosError(payload.error)) {
    return ({
      ...payload,
      error: {
        url: payload.error.config.url,
        status: payload.error.response?.status,
      },
    });
  }

  return payload;
};

export const jsonSerializer = (prettyPrint = false): Serializer => flow(
  (entry) => ({
    ...entry,
    payload: filterAxiosGarbageInPayload(entry.payload),
  }),
  (entry) => (
    JSON.stringify(entry, replaceError, prettyPrint ? 2 : undefined)
  ),
);

export const streamLogger = (
  stream: NodeJS.WritableStream,
  serializer: Serializer,
  logLevelName: string,
): Logger => {
  const configuredLevel = Level[logLevelName as LevelName] ?? Level.debug;
  return (level, message, payload = {}, date = new Date()) => {
    if (Level[level] > configuredLevel) {
      return;
    }
    const entry = {
      timestamp: date,
      level,
      message,
      payload,
    };

    stream.write(`${serializer(entry)}\n`);
  };
};
