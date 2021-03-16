import { updateGroupMeta } from '../../src/search-results-page/update-group-meta';
import { Doi } from '../../src/types/doi';
import {
  editorialCommunityReviewedArticle,
  userFollowedEditorialCommunity, userSavedArticle,
  userUnfollowedEditorialCommunity,
} from '../../src/types/domain-events';
import { GroupId } from '../../src/types/group-id';
import { toUserId } from '../../src/types/user-id';

describe('update-group-meta', () => {
  const groupId = new GroupId('123');
  const initial = { followerCount: 41, reviewCount: 27 };

  it('updates the meta when passed a UserFollowedEditorialCommunityEvent', () => {
    const event = userFollowedEditorialCommunity(toUserId('123456'), groupId);
    const result = updateGroupMeta(groupId)(initial, event);

    expect(result).toStrictEqual({ followerCount: 42, reviewCount: 27 });
  });

  it('updates the meta when passed a UserUnfollowedEditorialCommunityEvent', () => {
    const event = userUnfollowedEditorialCommunity(toUserId('123456'), groupId);
    const result = updateGroupMeta(groupId)(initial, event);

    expect(result).toStrictEqual({ followerCount: 40, reviewCount: 27 });
  });

  it('updates the meta when passed a EditorialCommunityReviewedArticle', () => {
    const event = editorialCommunityReviewedArticle(groupId, new Doi('10.1234/5678'), new Doi('10.1234/1234'));
    const result = updateGroupMeta(groupId)(initial, event);

    expect(result).toStrictEqual({ followerCount: 41, reviewCount: 28 });
  });

  it('does not update the meta when passed any other domain event', () => {
    const event = userSavedArticle(toUserId('123'), new Doi('10.1234/5678'));
    const result = updateGroupMeta(groupId)(initial, event);

    expect(result).toStrictEqual({ followerCount: 41, reviewCount: 27 });
  });
});
