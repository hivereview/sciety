import * as t from 'io-ts';
import { DateFromISOString } from 'io-ts-types/DateFromISOString';
import { DoiFromString } from './DoiFromString';
import { EventIdFromString } from './EventIdFromString';
import { GroupIdFromString } from './GroupIdFromString';
import { UserIdFromString } from './UserIdFromString';
import { reviewIdCodec } from '../review-id';

const articleRemovedFromUserListEvent = t.type({
  id: EventIdFromString,
  type: t.literal('ArticleRemovedFromUserList'),
  date: DateFromISOString,
  userId: UserIdFromString,
  articleId: DoiFromString,
});

const userFollowedEditorialCommunityEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserFollowedEditorialCommunity'),
  date: DateFromISOString,
  userId: UserIdFromString,
  editorialCommunityId: GroupIdFromString,
});

const userUnfollowedEditorialCommunityEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserUnfollowedEditorialCommunity'),
  date: DateFromISOString,
  userId: UserIdFromString,
  editorialCommunityId: GroupIdFromString,
});

const userFoundReviewHelpfulEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserFoundReviewHelpful'),
  date: DateFromISOString,
  userId: UserIdFromString,
  reviewId: reviewIdCodec,
});

const userFoundReviewNotHelpfulEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserFoundReviewNotHelpful'),
  date: DateFromISOString,
  userId: UserIdFromString,
  reviewId: reviewIdCodec,
});

const userRevokedFindingReviewHelpfulEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserRevokedFindingReviewHelpful'),
  date: DateFromISOString,
  userId: UserIdFromString,
  reviewId: reviewIdCodec,
});

const userRevokedFindingReviewNotHelpfulEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserRevokedFindingReviewNotHelpful'),
  date: DateFromISOString,
  userId: UserIdFromString,
  reviewId: reviewIdCodec,
});

const userSavedArticleEvent = t.type({
  id: EventIdFromString,
  type: t.literal('UserSavedArticle'),
  date: DateFromISOString,
  userId: UserIdFromString,
  articleId: DoiFromString,
});

export const domainEvent = t.union([
  articleRemovedFromUserListEvent,
  userFollowedEditorialCommunityEvent,
  userUnfollowedEditorialCommunityEvent,
  userFoundReviewHelpfulEvent,
  userFoundReviewNotHelpfulEvent,
  userRevokedFindingReviewHelpfulEvent,
  userRevokedFindingReviewNotHelpfulEvent,
  userSavedArticleEvent,
], 'type');

export const domainEvents = t.readonlyArray(domainEvent);
