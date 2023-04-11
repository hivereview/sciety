import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import { executeCommand } from './execute-command';
import { EditListDetailsCommand } from '../../commands';
import { replayListResource } from './replay-list-resource';
import { DomainEvent } from '../../../domain-events';
import { ErrorMessage } from '../../../types/error-message';

type ResourceAction = (command: EditListDetailsCommand)
=> (events: ReadonlyArray<DomainEvent>)
=> E.Either<ErrorMessage, ReadonlyArray<DomainEvent>>;

export const update: ResourceAction = (command) => (events) => pipe(
  events,
  replayListResource(command.listId),
  E.map(executeCommand(command)),
);
