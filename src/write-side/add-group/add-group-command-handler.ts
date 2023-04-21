import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { CommandHandler } from '../../types/command-handler';
import { executeCommand } from './execute-command';
import { AddGroupCommand } from '../commands';
import { CommitEvents, GetAllEvents } from '../../shared-ports';

type Ports = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type AddGroupCommandHandler = (
  adapters: Ports
) => CommandHandler<AddGroupCommand>;

export const addGroupCommandHandler: AddGroupCommandHandler = (
  adapters,
) => (
  command,
) => pipe(
  adapters.getAllEvents,
  T.map(executeCommand(command)),
  TE.chainTaskK(adapters.commitEvents),
);
