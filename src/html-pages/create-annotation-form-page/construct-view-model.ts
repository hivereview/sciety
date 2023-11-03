import * as O from 'fp-ts/Option';
import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { sequenceS } from 'fp-ts/Apply';
import { Queries } from '../../read-models';
import { ExternalQueries } from '../../third-parties';
import { ViewModel } from './view-model';
import { DataError } from '../../types/data-error';
import * as DE from '../../types/data-error';
import { ArticleId } from '../../types/article-id';
import { ListId } from '../../types/list-id';
import { toHtmlFragment } from '../../types/html-fragment';

export type Dependencies = Queries & ExternalQueries;

const getArticleTitle = (dependencies: Dependencies, articleId: ArticleId) => pipe(
  articleId,
  dependencies.fetchArticle,
  TE.map((articleDetails) => articleDetails.title),
);

const getListName = (dependencies: Dependencies, listId: ListId) => pipe(
  listId,
  dependencies.lookupList,
  O.map((list) => list.name),
  E.fromOption(() => DE.notFound),
  T.of,
);

export const constructViewModel = (
  articleId: ArticleId,
  listId: ListId,
  unrecoverableError: boolean,
  dependencies: Dependencies,
): TE.TaskEither<DataError, ViewModel> => pipe(
  {
    articleTitle: getArticleTitle(dependencies, articleId),
    listName: getListName(dependencies, listId),
  },
  sequenceS(TE.ApplyPar),
  TE.map((partial) => ({
    ...partial,
    articleId,
    listId,
    pageHeading: toHtmlFragment('Create an annotation'),
    unrecoverableError,
  })),
);
