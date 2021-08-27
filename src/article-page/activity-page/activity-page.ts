import * as O from 'fp-ts/Option';
import * as R from 'fp-ts/Reader';
import * as RT from 'fp-ts/ReaderTask';
import * as RTE from 'fp-ts/ReaderTaskEither';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { constant, flow, pipe } from 'fp-ts/function';
import striptags from 'striptags';
import { articleMetaTagContent } from './article-meta-tag-content';
import { FindReviewsForArticleDoi, FindVersionsForArticleDoi, getArticleFeedEventsByDateDescending } from './get-article-feed-events';
import { FetchReview } from './get-feed-events-content';
import { projectReviewResponseCounts } from './project-review-response-counts';
import { projectUserReviewResponse } from './project-user-review-response';
import { renderActivityPage } from './render-activity-page';
import {
  biorxivArticleVersionErrorFeedItem,
  medrxivArticleVersionErrorFeedItem,
} from './render-article-version-error-feed-item';
import { renderArticleVersionFeedItem } from './render-article-version-feed-item';
import { renderDescriptionMetaTagContent } from './render-description-meta-tag-content';
import { renderFeed } from './render-feed';
import { renderReviewFeedItem } from './render-review-feed-item';
import { DomainEvent } from '../../domain-events';
import { ArticleServer } from '../../types/article-server';
import * as DE from '../../types/data-error';
import { Doi } from '../../types/doi';
import { GroupId } from '../../types/group-id';
import { toHtmlFragment } from '../../types/html-fragment';
import { Page } from '../../types/page';
import { RenderPageError } from '../../types/render-page-error';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';
import { User } from '../../types/user';
import { projectHasUserSavedArticle } from '../project-has-user-saved-article';
import { renderSaveArticle } from '../render-save-article';
import { renderTweetThis } from '../render-tweet-this';

type ActivityPage = (params: Params) => (ports: Ports) => TE.TaskEither<RenderPageError, Page>;

type Params = {
  doi: Doi,
  user: O.Option<User>,
};

type GetArticleDetails = (doi: Doi) => TE.TaskEither<DE.DataError, {
  title: SanitisedHtmlFragment,
  abstract: SanitisedHtmlFragment, // TODO Use HtmlFragment as the HTML is stripped
  authors: ReadonlyArray<string>,
  server: ArticleServer,
}>;

type GetGroup = (groupId: GroupId) => TO.TaskOption<{
  name: string,
  avatarPath: string,
}>;

type Ports = {
  fetchArticle: GetArticleDetails,
  fetchReview: FetchReview,
  getGroup: GetGroup,
  findReviewsForArticleDoi: FindReviewsForArticleDoi,
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

const toErrorPage = (error: DE.DataError) => ({
  type: error,
  message: toHtmlFragment(`
    We’re having trouble finding this information.
    Ensure you have the correct URL, or try refreshing the page.
    You may need to come back later.
  `),
});

export const articleActivityPage: ActivityPage = flow(
  RTE.right,
  RTE.bind('userId', ({ user }) => pipe(user, O.map((u) => u.id), RTE.right)),
  RTE.bind('articleDetails', ({ doi }) => (ports: Ports) => pipe(doi, ports.fetchArticle)),
  RTE.bindW('feedItemsByDateDescending', ({ articleDetails, doi, userId }) => (ports: Ports) => pipe(
    articleDetails.server,
    (server) => getArticleFeedEventsByDateDescending(doi, server, userId)({
      ...ports,
      getGroup: flow(
        ports.getGroup,
        TO.getOrElse(() => {
          throw new Error('No such group');
        }),
      ),
      countReviewResponses: (reviewId) => projectReviewResponseCounts(reviewId)(ports.getAllEvents),
      getUserReviewResponse: (reviewId) => projectUserReviewResponse(reviewId, userId)(ports.getAllEvents),
    }),
    TE.rightTask,
  )),
  RTE.bindW('feed', ({ feedItemsByDateDescending }) => () => pipe(
    feedItemsByDateDescending,
    renderFeed(
      (feedItem) => {
        switch (feedItem.type) {
          case 'article-version':
            return renderArticleVersionFeedItem(feedItem);
          case 'article-version-error':
            return feedItem.server === 'medrxiv' ? medrxivArticleVersionErrorFeedItem : biorxivArticleVersionErrorFeedItem;
          case 'review':
            return renderReviewFeedItem(850)(feedItem);
        }
      },
    ),
    TE.right,
  )),
  RTE.bindW('hasUserSavedArticle', ({ doi, userId }) => pipe(
    userId,
    O.fold(
      constant(RT.of(false)),
      (u) => projectHasUserSavedArticle(doi, u),
    ),
    RTE.rightReaderTask,
    R.local((ports: Ports) => ports.getAllEvents),
  )),
  RTE.bindW('saveArticle', ({ doi, userId, hasUserSavedArticle }) => pipe(
    renderSaveArticle(doi, userId, hasUserSavedArticle),
    RTE.right,
  )),
  RTE.bindW('tweetThis', ({ doi }) => pipe(
    doi,
    renderTweetThis,
    RTE.right,
  )),
  RTE.bimap(
    toErrorPage,
    (components) => ({
      content: renderActivityPage(components),
      title: striptags(components.articleDetails.title),
      description: pipe(
        articleMetaTagContent(components.feedItemsByDateDescending),
        renderDescriptionMetaTagContent,
      ),
      openGraph: {
        title: striptags(components.articleDetails.title),
        description: striptags(components.articleDetails.abstract),
      },
    }),
  ),
);
