import { Middleware } from '@koa/router';
import compose from 'koa-compose';
import convertArticleAndReviewsToArticlePage from './middleware/convert-article-and-reviews-to-article-page';
import fetchArticleForArticlePage from './middleware/fetch-article-for-article-page';
import fetchReviewsForArticlePage from './middleware/fetch-reviews-for-article-page';
import renderArticlePage from './middleware/render-article-page';
import truncateReviewSummaries from './middleware/truncate-review-summaries';
import validateBiorxivDoi from './middleware/validate-biorxiv-doi';
import validateDoiParam from './middleware/validate-doi-param';
import { Adapters } from '../types/adapters';

export default (adapters: Adapters): Middleware => (
  compose([
    validateDoiParam(),
    validateBiorxivDoi(),
    fetchArticleForArticlePage(adapters.fetchArticle),
    fetchReviewsForArticlePage(adapters.reviewReferenceRepository, adapters.fetchReview),
    convertArticleAndReviewsToArticlePage(adapters.editorialCommunities),
    truncateReviewSummaries(),
    renderArticlePage(adapters.editorialCommunities),
  ])
);
