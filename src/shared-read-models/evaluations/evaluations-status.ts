import * as RA from 'fp-ts/ReadonlyArray';
import * as RM from 'fp-ts/ReadonlyMap';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { Json } from 'fp-ts/Json';
import * as D from 'fp-ts/Date';
import * as Ord from 'fp-ts/Ord';
import { ReadModel } from './handle-event';
import { RecordedEvaluation } from '../../types/recorded-evaluation';

const byDate: Ord.Ord<RecordedEvaluation> = pipe(
  D.Ord,
  Ord.contramap((evaluation) => evaluation.publishedAt),
);

export const evaluationsStatus = (readmodel: ReadModel) => (): Json => pipe(
  readmodel.byArticleId,
  RM.values(RA.getOrd(byDate)),
  RA.flatten,
  RA.map((evaluation) => evaluation.type),
  RA.filter((t) => O.isSome(t)),
  O.sequenceArray,
  (evaluationTypes) => ({
    curationStatements: pipe(
      evaluationTypes,
      O.match(
        () => 0,
        (evaluationType) => pipe(
          evaluationType,
          RA.filter((t) => t === 'curation-statement'),
          (found) => found.length,
        ),
      ),
    ),
    reviews: pipe(
      evaluationTypes,
      O.match(
        () => 0,
        (evaluationType) => pipe(
          evaluationType,
          RA.filter((t) => t === 'review'),
          (found) => found.length,
        ),
      ),
    ),
    authorResponse: pipe(
      evaluationTypes,
      O.match(
        () => 0,
        (evaluationType) => pipe(
          evaluationType,
          RA.filter((t) => t === 'author-response'),
          (found) => found.length,
        ),
      ),
    ),
  }),
);
