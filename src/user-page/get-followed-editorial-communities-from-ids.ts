import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { GetFollowedEditorialCommunities } from './render-follow-list';
import { EditorialCommunityId } from '../types/editorial-community-id';
import { UserId } from '../types/user-id';

export type GetFollowedEditorialCommunityIds = (userId: UserId) => T.Task<ReadonlyArray<EditorialCommunityId>>;

export type GetEditorialCommunity = (editorialCommunityId: EditorialCommunityId) => T.Task<O.Option<{
  id: EditorialCommunityId,
  name: string,
  avatarPath: string,
}>>;

export const getFollowedEditorialCommunitiesFromIds = (
  getFollowedEditorialCommunityIds: GetFollowedEditorialCommunityIds,
  getEditorialCommunity: GetEditorialCommunity,
): GetFollowedEditorialCommunities => (userId) => (
  pipe(
    userId,
    getFollowedEditorialCommunityIds,
    T.chain(T.traverseArray(getEditorialCommunity)),
    T.map(RA.compact),
  )
);
