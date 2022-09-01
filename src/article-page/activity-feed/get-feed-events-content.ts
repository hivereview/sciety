import { URL } from 'url';
import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { projectReviewResponseCounts } from './project-review-response-counts';
import { projectUserReviewResponse } from './project-user-review-response';
import { FeedItem } from './render-feed';
import { DomainEvent } from '../../domain-events';
import { getGroup } from '../../shared-read-models/groups';
import { constructReadModel } from '../../shared-read-models/groups/construct-read-model';
import { ArticleServer } from '../../types/article-server';
import { GroupId } from '../../types/group-id';
import { HtmlFragment } from '../../types/html-fragment';
import { ReviewId } from '../../types/review-id';
import * as RI from '../../types/review-id';
import { sanitise } from '../../types/sanitised-html-fragment';
import { UserId } from '../../types/user-id';

type ReviewEvent = {
  type: 'review',
  groupId: GroupId,
  reviewId: ReviewId,
  publishedAt: Date,
};

type ArticleVersionEvent = {
  type: 'article-version',
  source: URL,
  publishedAt: Date,
  version: number,
};

export type FeedEvent = ReviewEvent | ArticleVersionEvent;

export type FetchReview = (id: ReviewId) => TE.TaskEither<unknown, {
  fullText: HtmlFragment,
  url: URL,
}>;

const articleVersionToFeedItem = (
  server: ArticleServer,
  feedEvent: ArticleVersionEvent,
) => (
  T.of({ ...feedEvent, server })
);

export type Ports = {
  fetchReview: FetchReview,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

const reviewToFeedItem = (
  ports: Ports,
  feedEvent: ReviewEvent,
  userId: O.Option<UserId>,
) => pipe(
  {
    groupDetails: pipe(
      ports.getAllEvents,
      T.map(constructReadModel),
      T.map(getGroup(feedEvent.groupId)),
      TE.match(
        () => ({
          groupName: 'A group',
          groupHref: `/groups/${feedEvent.groupId}`,
          groupAvatar: '/static/images/sciety-logo.jpg',
        }),
        (group) => ({
          groupName: group.name,
          groupHref: `/groups/${group.slug}`,
          groupAvatar: group.avatarPath,
        }),
      ),
    ),
    review: pipe(
      feedEvent.reviewId,
      ports.fetchReview,
      TE.match(
        () => ({
          url: RI.inferredSourceUrl(feedEvent.reviewId),
          fullText: O.none,
        }),
        (review) => ({
          ...review,
          url: O.some(review.url),
          fullText: O.some(review.fullText),
        }),
      ),
    ),
    reviewResponses: pipe(
      ports.getAllEvents,
      T.map(projectReviewResponseCounts(feedEvent.reviewId)),
    ),
    userReviewResponse: projectUserReviewResponse(ports.getAllEvents)(feedEvent.reviewId, userId),
  },
  sequenceS(T.ApplyPar),
  T.map(({
    groupDetails, review, reviewResponses, userReviewResponse,
  }) => ({
    type: 'review' as const,
    id: feedEvent.reviewId,
    source: review.url,
    publishedAt: feedEvent.publishedAt,
    ...groupDetails,
    fullText: O.map(sanitise)(review.fullText),
    counts: reviewResponses,
    current: userReviewResponse,
  })),
);

type GetFeedEventsContent = (ports: Ports, server: ArticleServer, userId: O.Option<UserId>)
=> (feedEvents: ReadonlyArray<FeedEvent>)
=> T.Task<ReadonlyArray<FeedItem>>;

export const getFeedEventsContent: GetFeedEventsContent = (ports, server, userId) => (feedEvents) => {
  const toFeedItem = (feedEvent: FeedEvent): T.Task<FeedItem> => {
    switch (feedEvent.type) {
      case 'article-version':
        return articleVersionToFeedItem(server, feedEvent);
      case 'review':
        return reviewToFeedItem(ports, feedEvent, userId);
    }
  };
  return pipe(
    feedEvents,
    T.traverseArray(toFeedItem),
  );
};
