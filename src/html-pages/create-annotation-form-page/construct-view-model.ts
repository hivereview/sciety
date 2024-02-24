import * as O from 'fp-ts/Option';
import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { sequenceS } from 'fp-ts/Apply';
import { Queries } from '../../read-models/index.js';
import { ExternalQueries } from '../../third-parties/index.js';
import { ViewModel, UnrecoverableError } from './view-model.js';
import { DataError } from '../../types/data-error.js';
import * as DE from '../../types/data-error.js';
import { ArticleId } from '../../types/article-id.js';
import { ListId } from '../../types/list-id.js';
import { toHtmlFragment } from '../../types/html-fragment.js';
import * as EDOI from '../../types/expression-doi.js';

export type Dependencies = Queries & ExternalQueries;

const getArticleTitle = (dependencies: Dependencies, articleId: ArticleId) => pipe(
  EDOI.fromValidatedString(articleId.value),
  dependencies.fetchExpressionFrontMatter,
  TE.map((frontMatter) => frontMatter.title),
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
  dependencies: Dependencies,
  unrecoverableError?: UnrecoverableError,
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
    pageHeading: toHtmlFragment('Share your thoughts'),
    unrecoverableError: O.fromNullable(unrecoverableError),
  })),
);
