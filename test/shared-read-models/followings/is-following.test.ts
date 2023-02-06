import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { handleEvent, initialState } from '../../../src/shared-read-models/followings';
import { isFollowing } from '../../../src/shared-read-models/followings/is-following';
import { arbitraryUserId } from '../../types/user-id.helper';
import { userFollowedEditorialCommunity, userUnfollowedEditorialCommunity } from '../../../src/domain-events';

describe('is-following', () => {
  const groupId = arbitraryGroupId();
  const userId = arbitraryUserId();

  describe('when the user is not following the group', () => {
    const readmodel = pipe(
      [],
      RA.reduce(initialState(), handleEvent),
    );

    it('returns false', () => {
      expect(isFollowing(readmodel)(groupId)(userId)).toBe(false);
    });
  });

  describe('when the user followed and then unfollowed the group', () => {
    const readmodel = pipe(
      [
        userFollowedEditorialCommunity(userId, groupId),
        userUnfollowedEditorialCommunity(userId, groupId),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('returns false', () => {
      expect(isFollowing(readmodel)(groupId)(userId)).toBe(false);
    });
  });

  describe('when the user is following the group', () => {
    const readmodel = pipe(
      [
        userFollowedEditorialCommunity(userId, groupId),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it.failing('returns true', () => {
      expect(isFollowing(readmodel)(groupId)(userId)).toBe(true);
    });
  });
});
