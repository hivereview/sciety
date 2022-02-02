import axios from 'axios';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { constant, flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { GetTwitterResponse } from './get-twitter-response';
import { Logger } from '../../infrastructure/logger';
import * as DE from '../../types/data-error';
import { toUserId, UserId } from '../../types/user-id';

type UserDetails = {
  userId: UserId,
  avatarUrl: string,
  displayName: string,
  handle: string,
};

export type GetUserDetailsBatch = (
  userIds: ReadonlyArray<UserId>,
) => TE.TaskEither<DE.DataError, ReadonlyArray<UserDetails>>;

const twitterUserCodec = t.type({
  id: t.string,
  username: t.string,
  name: t.string,
  profile_image_url: t.string,
});

type TwitterUser = t.TypeOf<typeof twitterUserCodec>;

const codec = t.type({
  data: tt.optionFromNullable(t.array(twitterUserCodec)),
  errors: tt.optionFromNullable(t.unknown),
});

type TwitterResponse = t.TypeOf<typeof codec>;

const generateUrl = (userIds: ReadonlyArray<UserId>) => `https://api.twitter.com/2/users?ids=${userIds.join(',')}&user.fields=profile_image_url`;

const logErrors = (logger: Logger, userIds: ReadonlyArray<UserId>) => TE.map((response: TwitterResponse) => pipe(
  response,
  ({ errors }) => errors,
  O.map((errors) => logger(
    'warn',
    'Twitter returned an errors property',
    {
      uri: generateUrl(userIds),
      errors,
    },
  )),
  () => response,
));

const handleResponseErrors = (logger: Logger) => TE.mapLeft((error) => {
  logger('error', 'Unable to get Twitter response', { error });
  return (axios.isAxiosError(error) && error.response?.status === 400
    ? DE.notFound
    : DE.unavailable);
});

const decodeResponse = T.map(E.chainW(flow(
  codec.decode,
  E.mapLeft(() => DE.unavailable),
)));

const translateToUserDetails = RA.map((item: TwitterUser) => ({
  userId: toUserId(item.id),
  avatarUrl: item.profile_image_url,
  displayName: item.name,
  handle: item.username,
}));

export const getTwitterUserDetailsBatch = (
  getTwitterResponse: GetTwitterResponse,
  logger: Logger,
): GetUserDetailsBatch => RA.match(
  constant(TE.right([])),
  (userIds) => pipe(
    userIds,
    generateUrl,
    getTwitterResponse,
    handleResponseErrors(logger),
    decodeResponse,
    logErrors(logger, userIds),
    TE.map(({ data }) => data),
    TE.map(O.fold(
      () => [],
      translateToUserDetails,
    )),
  ),
);
