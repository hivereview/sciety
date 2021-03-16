import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { ArticleItem, GroupItem } from './data-types';
import { LimitedSet } from './fetch-extra-details';

type Matches = {
  query: string,
  groups: ReadonlyArray<GroupItem>,
  articles: {
    items: ReadonlyArray<ArticleItem>,
    total: number,
  },
};

export const selectSubsetToDisplay = (limit: number) => (state: Matches): LimitedSet => ({
  ...state,
  availableMatches: state.groups.length + state.articles.total,
  itemsToDisplay: pipe(
    [...state.groups, ...state.articles.items],
    RA.takeLeft(limit),
  ),
});
