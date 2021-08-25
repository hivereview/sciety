import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { constant, flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { GetTwitterResponse } from './get-twitter-response';
import * as DE from '../types/data-error';
import { UserId } from '../types/user-id';

type UserDetails = {
  avatarUrl: string,
  displayName: string,
  handle: string,
};

type GetTwitterUserDetailsBatch = (
  getTwitterResponse: GetTwitterResponse
) => (
  userIds: ReadonlyArray<UserId>
) => TE.TaskEither<DE.DataError, ReadonlyArray<UserDetails>>;

const codec = t.type({
  data: tt.optionFromNullable(t.array(t.type({
    username: t.string,
    name: t.string,
    profile_image_url: t.string,
  }))),
});

// ts-unused-exports:disable-next-line
export const getTwitterUserDetailsBatch: GetTwitterUserDetailsBatch = (
  getTwitterResponse,
) => (
  userIds,
) => pipe(
  userIds,
  RA.match(
    constant(TE.right([])),
    () => pipe(
      TE.tryCatch(async () => getTwitterResponse(`https://api.twitter.com/2/users?ids=${userIds.join(',')}&user.fields=profile_image_url`), () => DE.unavailable),
      T.map(E.chainW(flow(
        codec.decode,
        E.mapLeft(() => DE.unavailable),
      ))),
      T.map(E.chainW(({ data }) => pipe(
        data,
        E.fromOption(() => DE.notFound),
      ))),
      TE.map(
        RA.map((item) => ({
          avatarUrl: item.profile_image_url,
          displayName: item.name,
          handle: item.username,
        })),
      ),
    ),
  ),
);
