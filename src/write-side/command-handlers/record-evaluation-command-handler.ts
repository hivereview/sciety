import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { CommandHandler } from '../../types/command-handler';
import { recordPublication } from '../resources/evaluation';
import { RecordEvaluationPublicationCommand } from '../commands';
import { DependenciesForCommands } from '../dependencies-for-commands';

type RecordEvaluationCommandHandler = (
  dependencies: DependenciesForCommands
) => CommandHandler<RecordEvaluationPublicationCommand>;

export const recordEvaluationCommandHandler: RecordEvaluationCommandHandler = (
  dependencies,
) => (
  command,
) => pipe(
  dependencies.getAllEvents,
  T.map(recordPublication(command)),
  TE.chainTaskK(dependencies.commitEvents),
);
