import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import * as E from 'fp-ts/Either';
import { UserHandle } from '../../types/user-handle';
import { DomainEvent, isUserCreatedAccountEvent } from '../../domain-events';
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

export type UserResource = unknown;

type ReplayUserResource = (userId: UserId)
=> (events: ReadonlyArray<DomainEvent>)
=> E.Either<ErrorMessage, UserResource>;

export const replayUserResource: ReplayUserResource = () => () => E.left('nope' as ErrorMessage);
