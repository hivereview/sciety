import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { getArticleVersionEventsFromBiorxiv } from './biorxiv';
import { fetchAllPaperExpressions as fetchAllPaperExpressionsFromCrossref } from './crossref';
import { QueryExternalService } from './query-external-service';
import { Logger } from '../shared-ports';
import { ExternalQueries } from './external-queries';
import { expandMonolithicBiorxivOrMedrxivExpressions } from './expand-monolithic-biorxiv-or-medrxiv-expressions';
import * as PH from '../types/publishing-history';
import * as DE from '../types/data-error';
import { PaperExpression } from '../types/paper-expression';

const setupCrossrefHeaders = (bearerToken: O.Option<string>) => {
  const headers: Record<string, string> = { };
  if (O.isSome(bearerToken)) {
    headers['Crossref-Plus-API-Token'] = `Bearer ${bearerToken.value}`;
  }
  return headers;
};

const hasAtLeastOnePreprintExpression = (
  paperExpressions: ReadonlyArray<PaperExpression>,
): boolean => pipe(
  paperExpressions,
  RA.some((paperExpression) => paperExpression.expressionType === 'preprint'),
);

export const fetchPublishingHistory = (
  queryCrossrefService: QueryExternalService,
  queryExternalService: QueryExternalService,
  crossrefApiBearerToken: O.Option<string>,
  logger: Logger,
): ExternalQueries['fetchPublishingHistory'] => (expressionDoi) => pipe(
  fetchAllPaperExpressionsFromCrossref(
    queryCrossrefService(undefined, setupCrossrefHeaders(crossrefApiBearerToken)),
    logger,
    expressionDoi,
  ),
  TE.chain(expandMonolithicBiorxivOrMedrxivExpressions(
    getArticleVersionEventsFromBiorxiv({ queryExternalService, logger }),
  )),
  TE.filterOrElseW(
    hasAtLeastOnePreprintExpression,
    () => {
      logger('info', 'No preprints found in the publishing history', { expressionDoi });
      return DE.notFound;
    },
  ),
  TE.chainEitherKW((expressions) => pipe(
    expressions,
    PH.fromExpressions,
    E.mapLeft((publishingHistoryFailure) => {
      switch (publishingHistoryFailure) {
        case 'empty-publishing-history':
          logger('error', 'Publishing history is empty', { expressionDoi });
      }
      return DE.notFound;
    }),
  )),
  TE.mapLeft(() => DE.notFound),
);
