import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { Follower } from './augment-with-user-details';
import {
  DomainEvent,
  isUserFollowedEditorialCommunityEvent,
  isUserUnfollowedEditorialCommunityEvent,
  UserFollowedEditorialCommunityEvent,
  UserUnfollowedEditorialCommunityEvent,
} from '../../domain-events';
import { match } from '../../shared-components/guardMatch';
import { GroupId } from '../../types/group-id';
import { UserId } from '../../types/user-id';

type FindFollowers = (groupId: GroupId) => (events: ReadonlyArray<DomainEvent>) => ReadonlyArray<Follower>;

const calculateFollowerUserIds = (
  groupId: GroupId,
) => RA.reduce([], (state: ReadonlyArray<UserId>, event: DomainEvent) => {
  if (isUserFollowedEditorialCommunityEvent(event) && event.editorialCommunityId === groupId) {
    return [...state, event.userId];
  }
  if (isUserUnfollowedEditorialCommunityEvent(event) && event.editorialCommunityId === groupId) {
    return state.filter((userId) => userId !== event.userId);
  }
  return state;
});

const calculateFollowedGroupCounts = (
  events: ReadonlyArray<DomainEvent>,
  userIds: ReadonlyArray<UserId>,
) => pipe(
  events,
  // eslint-disable-next-line max-len
  RA.filter((e): e is UserFollowedEditorialCommunityEvent | UserUnfollowedEditorialCommunityEvent => isUserFollowedEditorialCommunityEvent(e) || isUserUnfollowedEditorialCommunityEvent(e)),
  RA.reduce(new Map<UserId, number>(), (state, event) => match(event)
    .whenAnd(
      isUserFollowedEditorialCommunityEvent,
      (e) => userIds.includes(e.userId),
      (e) => state.set(e.userId, (state.get(e.userId) ?? 0) + 1),
    )
    .whenAnd(
      isUserUnfollowedEditorialCommunityEvent,
      (e) => userIds.includes(e.userId),
      (e) => state.set(e.userId, (state.get(e.userId) ?? 0) - 1),
    )
    .run()),
);

export const findFollowers: FindFollowers = (groupId) => (events) => pipe(
  events,
  calculateFollowerUserIds(groupId),
  (userIds) => ({
    userIds,
    followedGroupCounts: calculateFollowedGroupCounts(events, userIds),
  }),
  ({ userIds, followedGroupCounts }) => pipe(
    userIds,
    RA.map((userId) => ({
      userId,
      followedGroupCount: followedGroupCounts.get(userId) ?? 0,
      listCount: 1,
    })),
    RA.reverse,
  ),
);
