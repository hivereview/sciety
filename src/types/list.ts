import { pipe } from 'fp-ts/function';
import * as N from 'fp-ts/number';
import * as Ord from 'fp-ts/Ord';
import * as S from 'fp-ts/string';
import * as EQ from 'fp-ts/Eq';
import * as RA from 'fp-ts/ReadonlyArray';
import { ListId } from './list-id';
import { ListOwnerId } from './list-owner-id';
import { ExpressionDoi } from './expression-doi';

export const Eq: EQ.Eq<List> = pipe(
  S.Eq,
  EQ.contramap((list) => list.id),
);

type ListEntry = {
  expressionDoi: ExpressionDoi,
  addedAtListVersion: number,
};

export type List = {
  id: ListId,
  name: string,
  description: string,
  entries: ReadonlyArray<ListEntry>,
  updatedAt: Date,
  ownerId: ListOwnerId,
};

const listEntriesByMostRecentlyAdded: Ord.Ord<List['entries'][number]> = pipe(
  N.Ord,
  Ord.reverse,
  Ord.contramap((entry) => entry.addedAtListVersion),
);

export const toExpressionDoisByMostRecentlyAdded = (entries: List['entries']): ReadonlyArray<ExpressionDoi> => pipe(
  entries,
  RA.sort(listEntriesByMostRecentlyAdded),
  RA.map((listEntry) => listEntry.expressionDoi),
);
