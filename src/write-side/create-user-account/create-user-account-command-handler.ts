import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { CreateUserAccountCommand } from '../commands/create-user-account';
import { setUpUserIfNecessary } from '../../user-account/set-up-user-if-necessary';
import { DomainEvent } from '../../domain-events';
import { CommitEvents } from '../../shared-ports';
import { CommandHandler } from '../../types/command-handler';
import { checkCommand } from './check-command';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

export type Ports = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type CreateUserAccountCommandHandler = (ports: Ports) => CommandHandler<CreateUserAccountCommand>;

export const createUserAccountCommandHandler: CreateUserAccountCommandHandler = (ports) => (command) => pipe(
  ports.getAllEvents,
  T.map(checkCommand(command)),
  TE.chainTaskK(() => pipe(
    ports.getAllEvents,
    T.map(setUpUserIfNecessary(command)),
    T.chain(ports.commitEvents),
  )),
);
