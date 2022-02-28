import { sequenceS } from 'fp-ts/Apply';
import * as A from 'fp-ts/Array';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Pool } from 'pg';
import { appendNewListsEventsFromDatabase } from './append-new-lists-events-from-database';
import { getListsEventsFromDatabase } from './get-lists-events-from-database';
import { Ports } from './ports';
import { byDate } from '../domain-events';
import {
  jsonSerializer, rTracerLogger, streamLogger,
} from '../infrastructure/logger';
import { listCreationEvents } from '../shared-read-models/lists/list-creation-data';

type Dependencies = {
  prettyLog: boolean,
  logLevel: string, // TODO: Make this a level name
};

export const createInfrastructure = (dependencies: Dependencies): TE.TaskEither<unknown, Ports> => pipe(
  {
    pool: new Pool(),
    logger: pipe(
      dependencies.prettyLog,
      jsonSerializer,
      (serializer) => streamLogger(process.stdout, serializer, dependencies.logLevel),
      rTracerLogger,
    ),
  },
  ({ pool, logger }) => (
    {
      eventsAvailableAtStartup: pipe(
        getListsEventsFromDatabase(pool, logger),
        TE.map((eventsFromDatabase) => [
          ...eventsFromDatabase,
          ...listCreationEvents,
        ]),
        TE.map(A.sort(byDate)),
      ),
      pool: TE.right(pool),
      logger: TE.right(logger),
    }
  ),
  sequenceS(TE.ApplyPar),
  TE.map(({ pool, logger, eventsAvailableAtStartup }) => ({
    getListsEvents: pipe(
      eventsAvailableAtStartup,
      appendNewListsEventsFromDatabase(pool, logger),
    ),
    logger,
  })),
);
