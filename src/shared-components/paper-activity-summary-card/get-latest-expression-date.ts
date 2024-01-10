import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import * as DE from '../../types/data-error';
import { ArticleServer } from '../../types/article-server';
import { ExpressionDoi } from '../../types/expression-doi';
import { Dependencies } from './dependencies';

type GetLatestExpressionDate = (
  dependencies: Dependencies,
) => (expressionDoi: ExpressionDoi, server: ArticleServer) => TO.TaskOption<Date>;

export const getLatestExpressionDate: GetLatestExpressionDate = (
  dependencies,
) => (
  expressionDoi, server,
) => pipe(
  dependencies.findAllExpressionsOfPaper(expressionDoi, server),
  T.map(
    E.chainOptionKW(
      () => DE.notFound,
    )((allExpressions) => pipe(
      allExpressions,
      RA.last,
      O.map((version) => version.publishedAt),
    )),
  ),
  TO.fromTaskEither,
);
