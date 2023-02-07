import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import * as PR from 'io-ts/PathReporter';
import { FetchData } from './fetch-data';
import { FetchEvaluations } from './update-all';
import { DoiFromString } from '../types/codecs/DoiFromString';
import { Doi, isDoi } from '../types/doi';

type Ports = {
  fetchData: FetchData,
};

const preReviewPreprint = t.type({
  handle: t.union([DoiFromString, t.string]),
  fullReviews: t.readonlyArray(t.type({
    createdAt: tt.DateFromISOString,
    doi: tt.optionFromNullable(DoiFromString),
    isPublished: t.boolean,
  })),
});

const preReviewResponse = t.type({
  data: t.readonlyArray(preReviewPreprint),
});

type PreReviewPreprint = t.TypeOf<typeof preReviewPreprint>;

type Review = {
  date: Date,
  handle: string | Doi,
  reviewDoi: O.Option<Doi>,
  isPublished: boolean,
};

const toEvaluationOrSkip = (preprint: Review) => pipe(
  preprint,
  E.right,
  E.filterOrElse(
    (p) => p.isPublished,
    () => ({ item: preprint.handle.toString(), reason: 'is not published' }),
  ),
  E.filterOrElse(
    (p): p is Review & { handle: Doi } => isDoi(p.handle),
    () => ({ item: preprint.handle.toString(), reason: 'not a DOI' }),
  ),
  E.filterOrElse(
    (p): p is Review & { handle: Doi, reviewDoi: O.Some<Doi> } => O.isSome(p.reviewDoi),
    () => ({ item: `${preprint.handle.toString()} / ${preprint.date.toISOString()}`, reason: 'review has no DOI' }),
  ),
  E.map((p) => ({
    date: p.date,
    articleDoi: p.handle.value,
    evaluationLocator: `doi:${p.reviewDoi.value.value}`,
    authors: [],
  })),
);

const toIndividualReviews = (preprint: PreReviewPreprint) => pipe(
  preprint.fullReviews,
  RA.map((review) => ({
    date: review.createdAt,
    handle: preprint.handle,
    reviewDoi: review.doi,
    isPublished: review.isPublished,
  })),
);

const identifyCandidates = (fetchData: FetchData) => pipe(
  fetchData<unknown>('https://www.prereview.org/api/v2/preprints', { Accept: 'application/json' }),
  TE.chainEitherK(flow(
    preReviewResponse.decode,
    E.mapLeft((errors) => PR.failure(errors).join('\n')),
  )),
  TE.map(flow(
    ({ data }) => data,
    RA.chain(toIndividualReviews),
  )),
);

export const fetchPrereviewEvaluations = (): FetchEvaluations => (ports: Ports) => pipe(
  identifyCandidates(ports.fetchData),
  TE.map(RA.map(toEvaluationOrSkip)),
  TE.map((parts) => ({
    evaluations: RA.rights(parts),
    skippedItems: RA.lefts(parts),
  })),
);
