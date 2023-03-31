import * as O from 'fp-ts/Option';
import * as R from 'fp-ts/Record';
import { identity, pipe } from 'fp-ts/function';
import { Doi } from '../../types/doi';
import { ReadModel, RecordedEvaluation } from './handle-event';

export type GetEvaluationsForDoi = (articleDoi: Doi) => ReadonlyArray<RecordedEvaluation>;

export const getEvaluationsForDoi = (readmodel: ReadModel): GetEvaluationsForDoi => (articleId) => pipe(
  readmodel,
  R.lookup(articleId.value),
  O.match(
    () => [],
    identity,
  ),
);
