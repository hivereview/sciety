import {
  userFoundReviewHelpful,
  userFoundReviewNotHelpful,
  userRevokedFindingReviewHelpful,
  userRevokedFindingReviewNotHelpful,
} from '../../src/domain-events';
import { reviewResponse } from '../../src/respond/review-response';
import { arbitraryReviewId } from '../types/review-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('review-response', () => {
  const userId = arbitraryUserId();
  const reviewId = arbitraryReviewId();

  it.each([
    ['no events', [], 'none'],
    ['event for other review', [
      userFoundReviewHelpful(userId, arbitraryReviewId()),
    ], 'none'],
    ['helpful', [
      userFoundReviewHelpful(userId, reviewId),
    ], 'helpful'],
    ['not-helpful', [
      userFoundReviewNotHelpful(userId, reviewId),
    ], 'not-helpful'],
    ['helpful event from other user', [
      userFoundReviewHelpful(arbitraryUserId(), reviewId),
    ], 'none'],
    ['not-helpful event from other user', [
      userFoundReviewNotHelpful(arbitraryUserId(), reviewId),
    ], 'none'],
    ['helpful, revoked helpful', [
      userFoundReviewHelpful(userId, reviewId),
      userRevokedFindingReviewHelpful(userId, reviewId),
    ], 'none'],
    ['not-helpful, revoked not-helpful', [
      userFoundReviewNotHelpful(userId, reviewId),
      userRevokedFindingReviewNotHelpful(userId, reviewId),
    ], 'none'],
    ['helpful, revoked helpful, helpful', [
      userFoundReviewHelpful(userId, reviewId),
      userRevokedFindingReviewHelpful(userId, reviewId),
      userFoundReviewHelpful(userId, reviewId),
    ], 'helpful'],
    ['helpful, revoked helpful, not-helpful', [
      userFoundReviewHelpful(userId, reviewId),
      userRevokedFindingReviewHelpful(userId, reviewId),
      userFoundReviewNotHelpful(userId, reviewId),
    ], 'not-helpful'],
    ['not-helpful, revoked not-helpful, helpful', [
      userFoundReviewNotHelpful(userId, reviewId),
      userRevokedFindingReviewNotHelpful(userId, reviewId),
      userFoundReviewHelpful(userId, reviewId),
    ], 'helpful'],
  ])('given %s', (_, events, expected) => {
    expect(reviewResponse(userId, reviewId)(events)).toStrictEqual(expected);
  });
});
