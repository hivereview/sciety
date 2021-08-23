import { userFollowedEditorialCommunity } from '../../../src/domain-events';
import { findFollowers } from '../../../src/group-page/followers/find-followers';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryUserId } from '../../types/user-id.helper';

describe('find-followers', () => {
  describe('when no users have followed the group', () => {
    it('return empty list', () => {
      const result = findFollowers(arbitraryGroupId())([]);

      expect(result).toStrictEqual([]);
    });
  });

  describe('when 1 user has followed the group', () => {
    const userId = arbitraryUserId();
    const groupId = arbitraryGroupId();
    const events = [
      userFollowedEditorialCommunity(userId, groupId),
    ];
    const result = findFollowers(groupId)(events);

    it('returns a list containing them as a follower', () => {
      expect(result).toStrictEqual([
        expect.objectContaining({
          userId,
        }),
      ]);
    });

    it('their followedGroupCount is 1', () => {
      expect(result).toStrictEqual([
        expect.objectContaining({
          followedGroupCount: 1,
        }),
      ]);
    });

    it('their listCount is 1', () => {
      expect(result).toStrictEqual([
        expect.objectContaining({
          listCount: 1,
        }),
      ]);
    });
  });

  describe('when 1 user has followed then unfollowed the group', () => {
    it.todo('return empty list');
  });

  describe('when 1 user has followed the group and another group', () => {
    it.todo('returns a list containing them as a follower');

    it.todo('their followedGroupCount is 2');
  });

  describe('when multiple users have followed the group', () => {
    it.todo('returns a list containing them as followers');

    it.todo('the list is ordered with most recently followed first');
  });
});
