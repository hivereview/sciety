import * as TE from 'fp-ts/TaskEither';
import { ListsEvent } from './lists-event';
import { Logger } from '../infrastructure/logger';
import * as DE from '../types/data-error';

export type Ports = {
  getListsEvents: TE.TaskEither<DE.DataError, ReadonlyArray<ListsEvent>>,
  eventsAvailableAtStartup: ReadonlyArray<ListsEvent>,
  getNewListsEvents: TE.TaskEither<DE.DataError, ReadonlyArray<ListsEvent>>,
  logger: Logger,
};
