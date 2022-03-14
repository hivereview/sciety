import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { identity, pipe } from 'fp-ts/function';
import * as PR from 'io-ts/PathReporter';
import { Pool } from 'pg';
import { EventRow, listsEventsCodec, selectAllListsEvents } from './events-table';
import { ListsEvent } from './lists-event';
import { Logger } from '../infrastructure/logger';
import * as DE from '../types/data-error';

const waitForTableToExist = async (pool: Pool, logger: Logger) => {
  logger('debug', 'Waiting for events table to exist');
  // eslint-disable-next-line no-loops/no-loops, no-constant-condition
  while (true) {
    const queryResult = await pool.query<{ exists: boolean }>(`
      SELECT EXISTS (
        SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'events'
      )
    `);
    if (queryResult.rows[0].exists) {
      break;
    }
    await T.delay(1000)(T.of(''))();
  }
};

const decodeListsEvents = (rows: ReadonlyArray<EventRow>) => pipe(
  rows,
  RA.map((row) => ({
    id: row.id,
    type: row.type,
    date: row.date,
    ...row.payload,
  })),
  listsEventsCodec.decode,
  E.mapLeft((errors) => new Error(PR.failure(errors).join('\n'))),
);

export const getListsEventsFromDatabase = (
  pool: Pool,
  logger: Logger,
): TE.TaskEither<DE.DataError, ReadonlyArray<ListsEvent>> => pipe(
  TE.tryCatch(
    async () => {
      await waitForTableToExist(pool, logger);
      return pool.query<EventRow>(selectAllListsEvents);
    },
    identity,
  ),
  TE.map((result) => result.rows),
  TE.chainFirstTaskK((rows) => T.of(logger('debug', 'Successfully retrieved rows from database', { count: rows.length }))),
  TE.chainEitherKW(decodeListsEvents),
  TE.chainFirstTaskK((rows) => T.of(logger('debug', 'Successfully decoded events from database', { count: rows.length }))),
  TE.mapLeft(() => DE.unavailable),
);
