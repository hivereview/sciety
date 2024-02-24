import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { Dependencies } from '../html-pages/paper-activity-page/construct-view-model/dependencies.js';
import { eqList, List } from '../read-models/lists/index.js';
import * as PH from '../types/publishing-history.js';

export const findAllListsContainingPaper = (
  dependencies: Dependencies,
) => (
  publishingHistory: PH.PublishingHistory,
): ReadonlyArray<List> => pipe(
  publishingHistory,
  PH.getAllExpressionDois,
  RA.map(dependencies.selectAllListsContainingExpression),
  RA.flatten,
  RA.uniq(eqList),
);
