import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import {
  FindReviewsForArticleDoi, populateArticleViewModel,
} from './populate-article-view-model';
import { renderSavedArticles } from './render-saved-articles';
import { informationUnavailable, noSavedArticles } from './static-messages';
import { renderUnsaveForm } from '../../save-article/render-unsave-form';
import { renderArticleCard } from '../../shared-components/article-card';
import { FindVersionsForArticleDoi, getLatestArticleVersionDate } from '../../shared-components/article-card/get-latest-article-version-date';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';
import { HtmlFragment } from '../../types/html-fragment';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';
import { UserId } from '../../types/user-id';

type FetchArticle = (doi: Doi) => TE.TaskEither<unknown, {
  doi: Doi,
  server: ArticleServer,
  title: SanitisedHtmlFragment,
  authors: ReadonlyArray<string>,
}>;

export type Ports = {
  fetchArticle: FetchArticle,
  findReviewsForArticleDoi: FindReviewsForArticleDoi,
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
};

type SavedArticles = (ports: Ports) => (
  dois: ReadonlyArray<Doi>,
  loggedInUser: O.Option<UserId>,
  listOwnerId: UserId,
) => T.Task<HtmlFragment>;

const controls = (loggedInUserId: O.Option<UserId>, listOwnerId: UserId, articleId: Doi) => pipe(
  loggedInUserId,
  O.filter((userId) => userId === listOwnerId),
  O.map(() => renderUnsaveForm(articleId)),
);

export const savedArticles: SavedArticles = (ports) => (dois, loggedInUser, listOwnerId) => pipe(
  dois,
  RNEA.fromReadonlyArray,
  TE.fromOption(() => noSavedArticles),
  TE.chainW(flow(
    T.traverseArray(ports.fetchArticle),
    T.map(RA.rights),
    T.map(RA.match(
      () => E.left(informationUnavailable),
      (values) => E.right(values),
    )),
  )),
  TE.chainTaskK(
    T.traverseArray(populateArticleViewModel({
      findReviewsForArticleDoi: ports.findReviewsForArticleDoi,
      getLatestArticleVersionDate: getLatestArticleVersionDate(ports.findVersionsForArticleDoi),
    })),
  ),
  TE.map(flow(
    RA.map((articleViewModel) => renderArticleCard(
      controls(loggedInUser, listOwnerId, articleViewModel.doi),
    )(articleViewModel)),
    renderSavedArticles,
  )),
  TE.toUnion,
);
