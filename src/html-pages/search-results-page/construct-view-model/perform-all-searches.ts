import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe, tupled } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { findGroups } from './find-groups';
import { Matches } from './select-subset-to-display';
import * as DE from '../../../types/data-error';
import { Dependencies } from './dependencies';

export const paramsCodec = t.type({
  query: t.string,
  cursor: tt.optionFromNullable(t.string),
  page: tt.optionFromNullable(tt.NumberFromString),
  evaluatedOnly: tt.optionFromNullable(t.unknown),
});

export type Params = t.TypeOf<typeof paramsCodec>;

type PerformAllSearches = (
  dependencies: Dependencies,
) => (pageSize: number) => (params: Params) => TE.TaskEither<DE.DataError, Matches>;

export const performAllSearches: PerformAllSearches = (dependencies) => (pageSize) => (params) => pipe(
  {
    query: TE.right(params.query),
    evaluatedOnly: TE.right(
      pipe(
        params.evaluatedOnly,
        O.isSome,
      ),
    ),
    pageSize: TE.right(pageSize),
    pageNumber: TE.right(params.page),
    category: TE.right('articles' as const),
    articles: pipe(
      [
        params.query,
        params.cursor,
        pipe(
          params.evaluatedOnly,
          O.isSome,
        ),
      ],
      tupled(dependencies.searchForArticles(pageSize)),
    ),
    groups: pipe(
      findGroups(dependencies, params.query),
      T.map(RA.map((groupId) => ({ id: groupId }))),
      TE.rightTask,
    ),
  },
  sequenceS(TE.ApplyPar),
);
