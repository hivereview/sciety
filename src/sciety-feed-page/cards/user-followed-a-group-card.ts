import { sequenceS } from 'fp-ts/Apply';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { ScietyFeedCard } from './sciety-feed-card';
import { UserFollowedEditorialCommunityEvent } from '../../domain-events';
import * as DE from '../../types/data-error';
import { Group } from '../../types/group';
import { GroupId } from '../../types/group-id';
import { toHtmlFragment } from '../../types/html-fragment';
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
  {
    group: pipe(
      event.editorialCommunityId,
      getGroup,
      TE.fromTaskOption(() => DE.notFound),
    ),
    userDetails: pipe(
      event.userId,
      getUserDetails,
      TE.orElse(() => TE.right({
        handle: 'A user',
        avatarUrl: '/static/images/sciety-logo.jpg',
      })),
    ),
  },
  sequenceS(TE.ApplyPar),
  TE.map(({ group, userDetails }) => ({
    linkUrl: `/groups/${group.slug}/about`,
    avatarUrl: userDetails.avatarUrl,
    titleText: `${userDetails.handle} followed a group`,
    date: event.date,
    details: {
      title: toHtmlFragment(group.name),
      content: toHtmlFragment(`<p>${group.shortDescription}</p>`),
    },
  })),
);
