import { URL } from 'url';
import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import { Evaluation } from './evaluation';
import { Ports as GetDateOfMostRecentArticleVersionPorts, getPublishedDateOfMostRecentArticleVersion } from './get-published-date-of-most-recent-article-version';
import { DomainEvent } from '../../domain-events';
import { getEvaluationsForDoi } from '../../shared-read-models/evaluations';
import { getGroup } from '../../shared-read-models/groups';
import * as DE from '../../types/data-error';
import { Doi } from '../../types/doi';
import { Group } from '../../types/group';
import { GroupId } from '../../types/group-id';
import { inferredSourceUrl, ReviewId } from '../../types/review-id';

export type DocmapModel = {
  articleId: Doi,
  group: Group,
  inputPublishedDate: O.Option<Date>,
  evaluations: RNEA.ReadonlyNonEmptyArray<Evaluation>,
};

type DocmapIdentifier = {
  articleId: Doi,
  groupId: GroupId,
};

type GenerateDocmapViewModel = (
  adapters: Ports
) => (
  docmapIdentifier: DocmapIdentifier
) => TE.TaskEither<DE.DataError, DocmapModel>;

type ReviewForArticle = {
  reviewId: ReviewId,
  groupId: GroupId,
  recordedAt: Date,
  publishedAt: Date,
  authors: ReadonlyArray<string>,
};

export type Ports = GetDateOfMostRecentArticleVersionPorts & {
  fetchReview: (reviewId: ReviewId) => TE.TaskEither<DE.DataError, { url: URL }>,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

const extendWithSourceUrl = (adapters: Ports) => (review: ReviewForArticle) => pipe(
  review.reviewId,
  inferredSourceUrl,
  O.fold(
    () => pipe(
      review.reviewId,
      adapters.fetchReview,
      TE.map((fetchedReview) => ({
        ...review,
        sourceUrl: fetchedReview.url,
      })),
    ),
    (sourceUrl) => TE.right({
      ...review,
      sourceUrl,
    }),
  ),
);

export const generateDocmapViewModel: GenerateDocmapViewModel = (adapters) => ({ articleId, groupId }) => pipe(
  {
    articleId: TE.right(articleId),
    evaluations: pipe(
      adapters.getAllEvents,
      TE.rightTask,
      TE.map(getEvaluationsForDoi(articleId)),
      TE.map(RA.filter((ev) => ev.groupId === groupId)),
      TE.chainW(TE.traverseArray(extendWithSourceUrl(adapters))),
      TE.chainEitherKW(flow(
        RNEA.fromReadonlyArray,
        E.fromOption(() => DE.notFound),
      )),
    ),
    inputPublishedDate: getPublishedDateOfMostRecentArticleVersion(adapters, articleId),
    group: pipe(
      adapters.getAllEvents,
      T.map(getGroup(groupId)),
    ),
  },
  sequenceS(TE.ApplyPar),
);
