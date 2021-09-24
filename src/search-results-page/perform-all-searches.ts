import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { constant, pipe, tupled } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { ArticleResults } from './data-types';
import { Matches } from './select-subset-to-display';
import * as DE from '../types/data-error';
import { GroupId } from '../types/group-id';

type FindArticles = (
  pageSize: number,
) => (query: string, cursor: O.Option<string>) => TE.TaskEither<DE.DataError, ArticleResults>;

type FindGroups = (q: string) => T.Task<ReadonlyArray<GroupId>>;

export type Ports = {
  findGroups: FindGroups,
  searchEuropePmc: FindArticles,
};

export const paramsCodec = t.type({
  query: t.string,
  category: tt.optionFromNullable(
    t.union([
      t.literal('groups'),
      t.literal('articles'),
    ]),
  ),
  cursor: tt.optionFromNullable(t.string),
  page: tt.optionFromNullable(tt.NumberFromString),
});

export type Params = t.TypeOf<typeof paramsCodec>;

type PerformAllSearches = (
  ports: Ports,
) => (pageSize: number) => (params: Params) => TE.TaskEither<DE.DataError, Matches>;

export const performAllSearches: PerformAllSearches = (ports) => (pageSize) => (params) => pipe(
  {
    query: TE.right(params.query),
    pageSize: TE.right(pageSize),
    pageNumber: TE.right(params.page),
    category: TE.right(O.getOrElse(constant('articles'))(params.category)),
    articles: pipe(
      [params.query, params.cursor],
      tupled(ports.searchEuropePmc(pageSize)),
    ),
    groups: pipe(
      params.query,
      ports.findGroups, // TODO: should only ask for 10 of n; should return a TE
      T.map(RA.map((groupId) => ({ id: groupId }))),
      TE.rightTask,
    ),
  },
  sequenceS(TE.ApplyPar),
);
