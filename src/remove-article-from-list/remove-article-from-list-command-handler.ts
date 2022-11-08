import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { executeCommand } from './execute-command';
import { removeArticleFromListCommandCodec } from '../commands';
import { validateInputShape } from '../commands/validate-input-shape';
import { CommitEvents, GetAllEvents } from '../shared-ports';
import { replayListAggregate } from '../shared-write-models/replay-list-aggregate';
import { CommandResult } from '../types/command-result';
import { ErrorMessage } from '../types/error-message';

type Ports = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type RemoveArticleFromListCommandHandler = (
  ports: Ports
) => (
  input: unknown,
) => TE.TaskEither<ErrorMessage, CommandResult>;

export const removeArticleFromListCommandHandler: RemoveArticleFromListCommandHandler = (
  ports,
) => (
  input,
) => pipe(
  input,
  validateInputShape(removeArticleFromListCommandCodec),
  TE.fromEither,
  TE.chainW((command) => pipe(
    ports.getAllEvents,
    TE.rightTask,
    TE.chainEitherK(replayListAggregate(command.listId)),
    TE.map(executeCommand(command)),
  )),
  TE.chainTaskK(ports.commitEvents),
);
