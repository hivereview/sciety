import { projectReviewResponseCounts } from '../../../src/article-page/activity-page/project-review-response-counts';
import {
  userFoundReviewHelpful,
  userFoundReviewNotHelpful,
  userRevokedFindingReviewHelpful,
  userRevokedFindingReviewNotHelpful,
} from '../../../src/domain-events';
import { arbitraryReviewId } from '../../types/review-id.helper';
import { arbitraryUserId } from '../../types/user-id.helper';

const reviewId = arbitraryReviewId();

describe('project-review-response-counts', () => {
  describe('given no events', () => {
    it('returns 0 `helpful` and 0 `not helpful`', async () => {
      const projected = await projectReviewResponseCounts(async () => [])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 0, notHelpfulCount: 0 });
    });
  });

  describe('given a user responded to a different review', () => {
    it('returns 0 `helpful` and 0 `not helpful`', async () => {
      const differentReviewId = arbitraryReviewId();
      const projected = await projectReviewResponseCounts(async () => [
        userFoundReviewHelpful(arbitraryUserId(), differentReviewId),
      ])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 0, notHelpfulCount: 0 });
    });
  });

  describe('given N different users responded helpful events', () => {
    it('returns N `helpful` and 0 `not helpful`', async () => {
      const projected = await projectReviewResponseCounts(async () => [
        userFoundReviewHelpful(arbitraryUserId(), reviewId),
        userFoundReviewHelpful(arbitraryUserId(), reviewId),
        userFoundReviewHelpful(arbitraryUserId(), reviewId),
      ])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 3, notHelpfulCount: 0 });
    });
  });

  describe('given a single user responded helpful and revoked the helpful response', () => {
    it('returns 0 `helpful` and 0 `not helpful`', async () => {
      const userId = arbitraryUserId();
      const projected = await projectReviewResponseCounts(async () => [
        userFoundReviewHelpful(userId, reviewId),
        userRevokedFindingReviewHelpful(userId, reviewId),
      ])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 0, notHelpfulCount: 0 });
    });
  });

  describe('given N different users responded not helpful events', () => {
    it('returns 0 `helpful` and N `not helpful`', async () => {
      const projected = await projectReviewResponseCounts(async () => [
        userFoundReviewNotHelpful(arbitraryUserId(), reviewId),
        userFoundReviewNotHelpful(arbitraryUserId(), reviewId),
        userFoundReviewNotHelpful(arbitraryUserId(), reviewId),
      ])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 0, notHelpfulCount: 3 });
    });
  });

  describe('given a single user responded: helpful, revoke helpful, not helpful, revoke not helpful', () => {
    it('returns 0 `helpful` and 0 `not helpful`', async () => {
      const userId = arbitraryUserId();
      const projected = await projectReviewResponseCounts(async () => [
        userFoundReviewHelpful(userId, reviewId),
        userRevokedFindingReviewHelpful(userId, reviewId),
        userFoundReviewNotHelpful(userId, reviewId),
        userRevokedFindingReviewNotHelpful(userId, reviewId),
      ])(reviewId)();

      expect(projected).toStrictEqual({ helpfulCount: 0, notHelpfulCount: 0 });
    });
  });
});
