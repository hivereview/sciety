import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import * as D from 'fp-ts/Date';
import * as DE from '../../types/data-error.js';
import { Dependencies } from './construct-view-model/dependencies.js';
import { ExpressionDoi } from '../../types/expression-doi.js';
import * as PH from '../../types/publishing-history.js';

const logWhenDuplicateExpressionDatesFound = (
  dependencies: Dependencies,
  expressionDoi: ExpressionDoi,
) => (history: PH.PublishingHistory) => {
  const uniqueDates = pipe(
    history.expressions,
    RA.map((expression) => expression.publishedAt),
    RA.uniq(D.Eq),
  );

  if (uniqueDates.length !== history.expressions.length) {
    dependencies.logger('debug', 'Expressions with duplicate dates found for a publishing history', { expressionDoi });
  }

  return history;
};

export const identifyLatestExpressionDoiOfTheSamePaper = (
  dependencies: Dependencies,
) => (
  expressionDoi: ExpressionDoi,
): TE.TaskEither<DE.DataError, ExpressionDoi> => pipe(
  expressionDoi,
  dependencies.fetchPublishingHistory,
  TE.map(logWhenDuplicateExpressionDatesFound(dependencies, expressionDoi)),
  TE.map((publishingHistory) => PH.getLatestExpression(publishingHistory).expressionDoi),
);
