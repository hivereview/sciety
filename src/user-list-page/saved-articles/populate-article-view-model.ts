import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import { flow, pipe } from 'fp-ts/function';
import { ArticleViewModel } from '../../shared-components/article-card';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';

type ArticleItem = {
  doi: Doi,
  server: ArticleServer,
  title: SanitisedHtmlFragment,
  authors: ReadonlyArray<string>,
};

export type FindReviewsForArticleDoi = (articleDoi: Doi) => T.Task<ReadonlyArray<{
  occurredAt: Date,
}>>;

type GetLatestArticleVersionDate = (articleDoi: Doi, server: ArticleServer) => TO.TaskOption<Date>;

type Ports = {
  findReviewsForArticleDoi: FindReviewsForArticleDoi,
  getLatestArticleVersionDate: GetLatestArticleVersionDate,
};

type GetLatestActivityDate = (reviews: ReadonlyArray<{ occurredAt: Date }>) => O.Option<Date>;

const getLatestActivityDate: GetLatestActivityDate = flow(
  RA.last,
  O.map(({ occurredAt }) => occurredAt),
);

export const populateArticleViewModel = (ports: Ports) => (item: ArticleItem): T.Task<ArticleViewModel> => pipe(
  item.doi,
  ports.findReviewsForArticleDoi,
  T.chain(flow(
    (reviews) => ({
      latestVersionDate: ports.getLatestArticleVersionDate(item.doi, item.server),
      latestActivityDate: pipe(reviews, getLatestActivityDate, T.of),
      evaluationCount: T.of(reviews.length),
    }),
    sequenceS(T.ApplyPar),
  )),
  T.map(({ latestVersionDate, latestActivityDate, evaluationCount }) => ({
    ...item,
    latestVersionDate,
    latestActivityDate,
    evaluationCount,
  })),
);
