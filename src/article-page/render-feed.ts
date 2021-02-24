import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { constant, pipe } from 'fp-ts/function';
import {
  biorxivArticleVersionErrorFeedItem,
  medrxivArticleVersionErrorFeedItem,
} from './render-article-version-error-feed-item';
import { ArticleVersionFeedItem, RenderArticleVersionFeedItem } from './render-article-version-feed-item';
import { RenderReviewFeedItem, ReviewFeedItem } from './render-review-feed-item';
import { templateListItems } from '../shared-components';
import { ArticleServer } from '../types/article-server';
import { Doi } from '../types/doi';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { UserId } from '../types/user-id';

type RenderFeed = (doi: Doi, server: ArticleServer, userId: O.Option<UserId>) => TE.TaskEither<'no-content', HtmlFragment>;

type ArticleVersionErrorFeedItem = { type: 'article-version-error', server: ArticleServer };

export type FeedItem = ReviewFeedItem | ArticleVersionFeedItem | ArticleVersionErrorFeedItem;

export type GetFeedItems = (doi: Doi, server: ArticleServer) => T.Task<ReadonlyArray<FeedItem>>;

export const createRenderFeed = (
  getFeedItems: GetFeedItems,
  renderReviewFeedItem: RenderReviewFeedItem,
  renderArticleVersionFeedItem: RenderArticleVersionFeedItem,
): RenderFeed => {
  const renderFeedItem = (feedItem: FeedItem, userId: O.Option<UserId>): T.Task<HtmlFragment> => {
    switch (feedItem.type) {
      case 'article-version':
        return T.of(renderArticleVersionFeedItem(feedItem));
      case 'article-version-error':
        return T.of(feedItem.server === 'medrxiv' ? medrxivArticleVersionErrorFeedItem : biorxivArticleVersionErrorFeedItem);
      case 'review':
        return renderReviewFeedItem(feedItem, userId);
    }
  };

  return (doi, server, userId) => pipe(
    getFeedItems(doi, server),
    T.chain(T.traverseArray((feedItem) => renderFeedItem(feedItem, userId))),
    T.map(RNEA.fromReadonlyArray),
    T.map(E.fromOption(constant('no-content' as const))),
    TE.map((items) => `
      <section class="article-feed">
        <h2>Feed</h2>

        <ol role="list" class="article-feed__list">
          ${templateListItems(items, 'article-feed__item')}
        </ol>
      </section>
    `),
    TE.map(toHtmlFragment),
  );
};
