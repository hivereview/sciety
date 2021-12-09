import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { Command, createAppropriateEvents } from './create-appropriate-events';
import { DomainEvent, RuntimeGeneratedEvent } from '../domain-events';
import { getGroup } from '../shared-read-models/groups';
import { GroupId } from '../types/group-id';

type ExecuteCommand = (command: Command)
=> (events: ReadonlyArray<DomainEvent>)
=> E.Either<unknown, ReadonlyArray<RuntimeGeneratedEvent>>;

type ConfirmGroupExists = (groupId: GroupId)
=> (events: ReadonlyArray<DomainEvent>)
=> E.Either<void, void>;

const ignoreResult = () => undefined;

const confirmGroupExists: ConfirmGroupExists = (groupId) => (events) => pipe(
  events,
  getGroup(groupId),
  E.bimap(ignoreResult, ignoreResult),
);

export const executeCommand: ExecuteCommand = (command) => (events) => pipe(
  events,
  E.right,
  E.chainFirst(confirmGroupExists(command.groupId)),
  E.map(createAppropriateEvents(command)),
);
