import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { ReadModel } from './handle-event';
import { isGroupId } from '../../types/list-owner-id';
import { List } from '../../types/list';

export type GetNonEmptyUserLists = () => ReadonlyArray<List>;

export const getNonEmptyUserLists = (
  readModel: ReadModel,
): GetNonEmptyUserLists => () => pipe(
  Object.values(readModel),
  RA.filter((list) => !isGroupId(list.ownerId)),
  RA.filter((list) => list.articleIds.length > 0),
);
