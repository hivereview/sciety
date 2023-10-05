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
import { ArticleId, isArticleId } from '../types/article-id';

type Ports = {
  fetchData: FetchData,
};

const preReviewPreprint = t.type({
  handle: t.union([DoiFromString, t.string]),
  fullReviews: t.readonlyArray(t.type({
    createdAt: tt.DateFromISOString,
    doi: tt.optionFromNullable(DoiFromString),
    isPublished: t.boolean,
    authors: t.readonlyArray(t.type({
      name: t.string,
    })),
  })),
});

const preReviewResponse = t.type({
  data: t.readonlyArray(preReviewPreprint),
});

type PreReviewPreprint = t.TypeOf<typeof preReviewPreprint>;

type Review = {
  date: Date,
  handle: string | ArticleId,
  reviewDoi: O.Option<ArticleId>,
  isPublished: boolean,
  authors: ReadonlyArray<string>,
};

const toEvaluationOrSkip = (preprint: Review) => pipe(
  preprint,
  E.right,
  E.filterOrElse(
    (p): p is Review & { handle: ArticleId } => isArticleId(p.handle),
    () => ({ item: preprint.handle.toString(), reason: 'not a DOI' }),
  ),
  E.filterOrElse(
    (p): p is Review & { handle: ArticleId, reviewDoi: O.Some<ArticleId> } => O.isSome(p.reviewDoi),
    () => ({ item: `${preprint.handle.toString()} / ${preprint.date.toISOString()}`, reason: 'review has no DOI' }),
  ),
  E.filterOrElse(
    (p) => p.isPublished,
    () => ({ item: preprint.handle.toString(), reason: 'is not published' }),
  ),
  E.map((p) => ({
    date: p.date,
    articleDoi: p.handle.value,
    evaluationLocator: `doi:${p.reviewDoi.value.value}`,
    authors: p.authors,
  })),
);

const toIndividualReviews = (preprint: PreReviewPreprint): ReadonlyArray<Review> => pipe(
  preprint.fullReviews,
  RA.map((review) => ({
    date: review.createdAt,
    handle: preprint.handle,
    reviewDoi: review.doi,
    isPublished: review.isPublished,
    authors: pipe(
      review.authors,
      RA.map((author) => author.name),
    ),
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
