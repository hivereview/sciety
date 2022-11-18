import * as A from 'fp-ts/Array';
import * as D from 'fp-ts/Date';
import * as Ord from 'fp-ts/Ord';
import { pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import * as t from 'io-ts';
import { annotationCreatedEventCodec } from './annotation-created-event';
import { articleAddedToListEventCodec } from './article-added-to-list-event';
import { articleRemovedFromListEventCodec } from './article-removed-from-list-event';
import { evaluationRecordedEventCodec } from './evaluation-recorded-event';
import { groupJoinedEventCodec } from './group-joined-event';
import { listCreatedEventCodec } from './list-created-event';
import { subjectAreaRecordedEventCodec } from './subject-area-recorded-event';
import { userCreatedAccountEventCodec } from './user-created-account-event';
import { userFollowedEditorialCommunityEventCodec } from './user-followed-editorial-community-event';
import { userFoundReviewHelpfulEventCodec } from './user-found-review-helpful-event';
import { userFoundReviewNotHelpfulEventCodec } from './user-found-review-not-helpful-event';
import { userRevokedFindingReviewHelpfulEventCodec } from './user-revoked-finding-review-helpful-event';
import { userRevokedFindingReviewNotHelpfulEventCodec } from './user-revoked-finding-review-not-helpful-event';
import { userSavedArticleEventCodec } from './user-saved-article-event';
import { userUnfollowedEditorialCommunityEventCodec } from './user-unfollowed-editorial-community-event';
import { userUnsavedArticleEventCodec } from './user-unsaved-article-event';

export const domainEventCodec = t.union([
  annotationCreatedEventCodec,
  articleAddedToListEventCodec,
  articleRemovedFromListEventCodec,
  subjectAreaRecordedEventCodec,
  evaluationRecordedEventCodec,
  groupJoinedEventCodec,
  listCreatedEventCodec,
  userCreatedAccountEventCodec,
  userFollowedEditorialCommunityEventCodec,
  userUnfollowedEditorialCommunityEventCodec,
  userFoundReviewHelpfulEventCodec,
  userFoundReviewNotHelpfulEventCodec,
  userRevokedFindingReviewHelpfulEventCodec,
  userRevokedFindingReviewNotHelpfulEventCodec,
  userSavedArticleEventCodec,
  userUnsavedArticleEventCodec,
], 'type');

export type DomainEvent = t.TypeOf<typeof domainEventCodec>;

const byDate: Ord.Ord<DomainEvent> = pipe(
  D.Ord,
  Ord.contramap((event) => event.date),
);

const byUuid: Ord.Ord<DomainEvent> = pipe(
  S.Ord,
  Ord.contramap((event) => event.id),
);

export const sort = A.sortBy([byDate, byUuid]);
