import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { DomainEvent, userDetailsUpdated } from '../../domain-events';
import { UpdateUserDetailsCommand } from '../commands/update-user-details';
import { UserResource } from '../resources/user-resource';

const updateAvatarUrl = (command: UpdateUserDetailsCommand, userResource: UserResource) => pipe(
  command.avatarUrl,
  O.fromNullable,
  O.match(
    () => [],
    (avatarUrl) => ((userResource.avatarUrl === avatarUrl)
      ? []
      : [userDetailsUpdated(command.userId, avatarUrl)]),
  ),
);
const updateDisplayName = (command: UpdateUserDetailsCommand, userResource: UserResource) => pipe(
  command.displayName,
  O.fromNullable,
  O.match(
    () => [],
    (displayName) => ((userResource.displayName === displayName)
      ? []
      : [userDetailsUpdated(command.userId, undefined, displayName)]),
  ),
);

type ExecuteCommand = (command: UpdateUserDetailsCommand)
=> (userResource: UserResource)
=> ReadonlyArray<DomainEvent>;

export const executeCommand: ExecuteCommand = (command) => (userResource) => pipe(
  [
    updateAvatarUrl(command, userResource),
    updateDisplayName(command, userResource),
  ],
  RA.flatten,
);
