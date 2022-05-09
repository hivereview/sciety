import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import { constant, pipe } from 'fp-ts/function';
import { getFeedEventsContent, Ports as GetFeedEventsContentPorts } from './activity-feed/get-feed-events-content';
import { handleArticleVersionErrors } from './activity-feed/handle-article-version-errors';
import { FeedItem } from './activity-feed/render-feed';
import { mergeFeeds } from './merge-feeds';
import { DomainEvent } from '../../domain-events';
import { getEvaluationsForDoi } from '../../shared-read-models/evaluations';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';
import { UserId } from '../../types/user-id';

export type FindVersionsForArticleDoi = (
  doi: Doi,
  server: ArticleServer
) => TO.TaskOption<RNEA.ReadonlyNonEmptyArray<{
  source: URL,
  publishedAt: Date,
  version: number,
}>>;

type Ports = GetFeedEventsContentPorts & {
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

type GetArticleFeedEventsByDateDescending = (
  ports: Ports
) => (
  doi: Doi,
  server: ArticleServer,
  userId: O.Option<UserId>,
) => T.Task<RNEA.ReadonlyNonEmptyArray<FeedItem>>;

export const getArticleFeedEventsByDateDescending: GetArticleFeedEventsByDateDescending = (
  ports,
) => (
  doi, server, userId,
) => pipe(
  [
    pipe(
      ports.getAllEvents,
      T.map(getEvaluationsForDoi(doi)),
      T.map(RA.map((review) => ({ type: 'review', ...review } as const))),
    ),
    pipe(
      ports.findVersionsForArticleDoi(doi, server),
      TO.matchW(
        constant([]),
        RNEA.map((version) => ({ type: 'article-version', ...version } as const)),
      ),
    ),
  ] as const,
  mergeFeeds,
  T.chain(getFeedEventsContent(ports, server, userId)),
  T.map(handleArticleVersionErrors(server)),
);
