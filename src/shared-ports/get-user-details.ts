import * as TE from 'fp-ts/TaskEither';
import * as DE from '../types/data-error';
import { UserId } from '../types/user-id';

export type UserDetails = {
  avatarUrl: string,
  displayName: string,
  handle: string,
  userId: UserId,
};

export type GetUserDetails = (userId: UserId) => TE.TaskEither<DE.DataError, UserDetails>;
