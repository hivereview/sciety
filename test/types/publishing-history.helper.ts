import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';

import * as PH from '../../src/types/publishing-history.js';
import { shouldNotBeCalled } from '../should-not-be-called.js';
import { arbitraryPaperExpression } from './paper-expression.helper.js';
import { ExpressionDoi } from '../../src/types/expression-doi.js';
import { arbitraryExpressionDoi } from './expression-doi.helper.js';

type ArbitraryPublishingHistoryOnlyPreprints = (overrides?: {
  earliestExpressionDoi?: ExpressionDoi,
  latestExpressionDoi?: ExpressionDoi,
})
=> PH.PublishingHistory;

export const arbitraryPublishingHistoryOnlyPreprints: ArbitraryPublishingHistoryOnlyPreprints = (
  overrides = {
    earliestExpressionDoi: arbitraryExpressionDoi(),
    latestExpressionDoi: arbitraryExpressionDoi(),
  },
) => pipe(
  [
    {
      ...arbitraryPaperExpression(),
      expressionType: 'preprint',
      expressionDoi: overrides.earliestExpressionDoi ?? arbitraryExpressionDoi(),
      publishedAt: new Date('2000-01-01'),
    },
    {
      ...arbitraryPaperExpression(),
      expressionType: 'preprint',
      expressionDoi: overrides.latestExpressionDoi ?? arbitraryExpressionDoi(),
      publishedAt: new Date('2020-01-01'),
    },
  ],
  PH.fromExpressions,
  E.getOrElseW(shouldNotBeCalled),
);
