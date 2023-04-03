import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { RecordArticleVersionCommand } from '../commands';
import { CommitEvents, GetAllEvents } from '../../shared-ports';
import { CommandHandler } from '../../types/command-handler';

export type Ports = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type RecordArticleVersionCommandHandler = (
  adapters: Ports
) => CommandHandler<RecordArticleVersionCommand>;

export const recordArticleVersionCommandHandler: RecordArticleVersionCommandHandler = (
  adapters,
) => (
  command,
) => pipe(
  adapters.getAllEvents,
  T.map(() => []),
  T.chain(adapters.commitEvents),
  TE.rightTask,
);
