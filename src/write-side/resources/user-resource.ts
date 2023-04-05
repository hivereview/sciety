import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import * as E from 'fp-ts/Either';
import { UserHandle } from '../../types/user-handle';
import { DomainEvent, isUserCreatedAccountEvent, UserCreatedAccountEvent } from '../../domain-events';
import { UserId } from '../../types/user-id';
import { ErrorMessage } from '../../types/error-message';

export const exists = (userHandle: UserHandle) => (events: ReadonlyArray<DomainEvent>) => pipe(
  events,
  RA.filter(isUserCreatedAccountEvent),
  RA.map((event) => event.handle),
  RA.filter((handle) => handle.toLowerCase() === userHandle.toLowerCase()),
  RA.match(
    () => false,
    () => true,
  ),
);

export type UserResource = { avatarUrl: string };

type ReplayUserResource = (userId: UserId)
=> (events: ReadonlyArray<DomainEvent>)
=> E.Either<ErrorMessage, UserResource>;

const resourceFromCreationEvent = (event: UserCreatedAccountEvent) => ({ avatarUrl: event.avatarUrl });

export const replayUserResource: ReplayUserResource = (userId) => (events) => pipe(
  events,
  RA.filter(isUserCreatedAccountEvent),
  RA.filter((event) => event.userId === userId),
  RA.match(
    () => E.left('userId not found' as ErrorMessage),
    (relevantEvents) => pipe(
      relevantEvents[0],
      resourceFromCreationEvent,
      E.right,
    ),
  ),
);
