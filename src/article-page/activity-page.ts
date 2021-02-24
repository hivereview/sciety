import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { constant, flow, pipe } from 'fp-ts/function';
import striptags from 'striptags';
import { ensureBiorxivDoi } from './ensure-biorxiv-doi';
import { FindVersionsForArticleDoi, getArticleFeedEvents } from './get-article-feed-events';
import { GetReview } from './get-feed-events-content';
import { projectHasUserSavedArticle } from './project-has-user-saved-article';
import { createProjectReviewResponseCounts } from './project-review-response-counts';
import { createProjectUserReviewResponse } from './project-user-review-response';
import { renderActivityPage } from './render-activity-page';
import { renderArticleVersionFeedItem } from './render-article-version-feed-item';
import { createRenderFeed } from './render-feed';
import { createRenderReviewFeedItem } from './render-review-feed-item';
import { createRenderReviewResponses } from './render-review-responses';
import { renderSaveArticle } from './render-save-article';
import { renderTweetThis } from './render-tweet-this';
import { ArticleServer } from '../types/article-server';
import { Doi } from '../types/doi';
import { DomainEvent } from '../types/domain-events';
import { EditorialCommunityId } from '../types/editorial-community-id';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { RenderPageError } from '../types/render-page-error';
import { ReviewId } from '../types/review-id';
import { SanitisedHtmlFragment } from '../types/sanitised-html-fragment';
import { User } from '../types/user';
import { UserId } from '../types/user-id';

type ActivityPage = (params: Params) => TE.TaskEither<RenderPageError, Page>;

type Page = {
  title: string,
  content: HtmlFragment,
  openGraph: {
    title: string,
    description: string,
  },
};

type Params = {
  doi?: string,
  user: O.Option<User>,
};

type FindReviewsForArticleDoi = (articleVersionDoi: Doi) => T.Task<ReadonlyArray<{
  reviewId: ReviewId,
  editorialCommunityId: EditorialCommunityId,
  occurredAt: Date,
}>>;

type ArticleDetails = {
  title: SanitisedHtmlFragment,
  abstract: SanitisedHtmlFragment, // TODO Use HtmlFragment as the HTML is stripped
  authors: Array<string>,
  server: ArticleServer,
};

type GetArticleDetails = (doi: Doi) => TE.TaskEither<'not-found' | 'unavailable', ArticleDetails>;

type Ports = {
  fetchArticle: GetArticleDetails,
  fetchReview: GetReview,
  getEditorialCommunity: (editorialCommunityId: EditorialCommunityId) => T.Task<O.Option<{
    name: string,
    avatarPath: string,
  }>>,
  findReviewsForArticleDoi: FindReviewsForArticleDoi,
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

const getUserId = (user: O.Option<User>): O.Option<UserId> => pipe(
  user,
  O.map((u) => u.id),
);

const toErrorPage = (error: 'not-found' | 'unavailable'): RenderPageError => {
  switch (error) {
    case 'not-found':
      return {
        type: 'not-found',
        message: toHtmlFragment(`
          We’re having trouble finding this information.
          Ensure you have the correct URL, or try refreshing the page.
          You may need to come back later.
        `),
      };
    case 'unavailable':
      return {
        type: 'unavailable',
        message: toHtmlFragment(`
          We’re having trouble finding this information.
          Ensure you have the correct URL, or try refreshing the page.
          You may need to come back later.
        `),
      };
  }
};

export const articleActivityPage = (ports: Ports): ActivityPage => {
  const countReviewResponses = createProjectReviewResponseCounts(ports.getAllEvents);
  const renderFeed = createRenderFeed(
    getArticleFeedEvents(
      ports.findReviewsForArticleDoi,
      ports.findVersionsForArticleDoi,
      ports.fetchReview,
      ports.getEditorialCommunity,
    ),
    createRenderReviewFeedItem(
      150,
      createRenderReviewResponses(
        countReviewResponses,
        createProjectUserReviewResponse(ports.getAllEvents),
      ),
    ),
    renderArticleVersionFeedItem,
  );

  return (params) => pipe(
    params.doi ?? '',
    ensureBiorxivDoi,
    O.fold(
      () => TE.left({
        type: 'not-found',
        message: toHtmlFragment(`${params.doi ?? 'Article'} not found`),
      }),
      flow(
        TE.right,
        TE.bindTo('doi'),
        TE.bind('userId', () => pipe(params.user, getUserId, TE.right)),
        TE.bind('articleDetails', ({ doi }) => pipe(doi, ports.fetchArticle)),
        TE.bindW('feed', ({ articleDetails, doi, userId }) => pipe(
          articleDetails.server,
          (server) => renderFeed(doi, server, userId),
          TE.orElse(flow(constant(''), TE.right)),
          TE.map(toHtmlFragment),
        )),
        TE.bindW('saveArticle', ({ doi, userId }) => pipe(
          renderSaveArticle(projectHasUserSavedArticle(ports.getAllEvents))(doi, userId),
          TE.rightTask,
        )),
        TE.bindW('tweetThis', ({ doi }) => pipe(
          doi,
          renderTweetThis,
          TE.right,
        )),
        TE.bimap(
          toErrorPage,
          (components) => ({
            content: renderActivityPage(components),
            title: `${striptags(components.articleDetails.title)}`,
            openGraph: {
              title: striptags(components.articleDetails.title),
              description: striptags(components.articleDetails.abstract),
            },
          }),
        ),
      ),
    ),
  );
};
