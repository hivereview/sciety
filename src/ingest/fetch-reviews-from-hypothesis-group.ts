import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Evaluation } from './evaluations';
import { FetchData } from './fetch-data';
import { medrxivOrBiorxivLinkToDoi } from './medrxiv-or-biorxiv-link-to-doi';
import { daysAgo } from './time';
import { FetchEvaluations, SkippedItem } from './update-all';
import * as Hyp from '../third-parties/hypothesis';

// ts-unused-exports:disable-next-line
export const toEvaluation = (row: Hyp.Annotation): E.Either<SkippedItem, Evaluation> => pipe(
  row.uri,
  medrxivOrBiorxivLinkToDoi,
  E.bimap(
    (reason) => ({ item: row.uri, reason }),
    (articleDoi) => ({
      date: new Date(row.created),
      articleDoi,
      evaluationLocator: `hypothesis:${row.id}`,
      authors: [],
    }),
  ),
);

type Ports = {
  fetchData: FetchData,
};

export const fetchReviewsFromHypothesisGroup = (publisherGroupId: string): FetchEvaluations => (ports: Ports) => pipe(
  publisherGroupId,
  Hyp.fetchEvaluationsByGroupSince(daysAgo(5), ports.fetchData),
  TE.map(RA.map(toEvaluation)),
  TE.map((parts) => ({
    evaluations: RA.rights(parts),
    skippedItems: RA.lefts(parts),
  })),
);
