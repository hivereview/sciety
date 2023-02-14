/* eslint-disable @typescript-eslint/no-unused-vars */
import * as RM from 'fp-ts/ReadonlyMap';
import * as O from 'fp-ts/Option';
import * as S from 'fp-ts/string';
import { pipe } from 'fp-ts/function';
import { Doi } from '../../types/doi';
import { ReadModel } from './handle-event';

// ts-unused-exports:disable-next-line
export const getActivityForDoi = (readmodel: ReadModel) => (articleId: Doi) => pipe(
  readmodel,
  RM.lookup(S.Eq)(articleId.value),
  O.match(
    () => ({
      articleId,
      latestActivityDate: O.none,
      evaluationCount: 0,
      listMembershipCount: 0,
    }),
    (act) => ({
      articleId: act.articleId,
      latestActivityDate: act.latestActivityDate,
      evaluationCount: act.evaluationCount,
      listMembershipCount: act.listMembershipCount,
    }),
  ),
);
