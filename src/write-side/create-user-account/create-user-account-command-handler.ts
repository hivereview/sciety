import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { CreateUserAccountCommand } from '../commands/create-user-account';
import { DomainEvent } from '../../domain-events';
import { CommitEvents } from '../../shared-ports';
import { CommandHandler } from '../../types/command-handler';
import { create } from '../resources/user';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

export type Ports = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type CreateUserAccountCommandHandler = (dependencies: Ports) => CommandHandler<CreateUserAccountCommand>;

export const createUserAccountCommandHandler: CreateUserAccountCommandHandler = (dependencies) => (command) => pipe(
  dependencies.getAllEvents,
  T.map(create(command)),
  TE.chainTaskK(dependencies.commitEvents),
);
