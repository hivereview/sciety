import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { ScietyFeedCard } from './sciety-feed-card';
import { UserFollowedEditorialCommunityEvent } from '../../domain-events';
import * as DE from '../../types/data-error';
import { Group } from '../../types/group';
import { GroupId } from '../../types/group-id';
import { UserId } from '../../types/user-id';

type GetGroup = (id: GroupId) => TO.TaskOption<Group>;

type GetUserDetails = (userId: UserId) => TE.TaskEither<DE.DataError, {
  handle: string,
  avatarUrl: string,
}>;

type UserFollowedAGroupCard = (
  getUserDetails: GetUserDetails,
  getGroup: GetGroup,
) => (event: UserFollowedEditorialCommunityEvent) => TE.TaskEither<DE.DataError, ScietyFeedCard>;

// ts-unused-exports:disable-next-line
export const userFollowedAGroupCard: UserFollowedAGroupCard = (getUserDetails, getGroup) => (event) => pipe(
  event.editorialCommunityId,
  getGroup,
  TE.fromTaskOption(() => DE.notFound),
  TE.chainTaskK(() => pipe(
    event.userId,
    getUserDetails,
    TE.match(
      () => ({
        titleText: 'A user followed a group',
        linkUrl: '',
        avatarUrl: '/static/images/sciety-logo.jpg',
        date: event.date,
      }),
      ({ handle, avatarUrl }) => ({
        titleText: `${handle} followed a group`,
        linkUrl: '',
        avatarUrl,
        date: event.date,
      }),
    ),
  )),
);
