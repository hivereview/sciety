import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { OurListsViewModel } from './render-lists';
import { List } from '../../shared-read-models/lists';
import { toListCardViewModel } from '../lists/to-list-card-view-model';

const maxSlimlineCards = 3;

type ToOurListsViewModel = (groupSlug: string) => (lists: ReadonlyArray<List>) => OurListsViewModel;

const truncatedView = <T>(slimlineCards: ReadonlyArray<T>, groupSlug: string) => (
  {
    slimlineCards: RA.takeLeft(maxSlimlineCards)(slimlineCards),
    viewAllListsUrl: O.some(`/groups/${groupSlug}/lists`),
  }
);

export const toOurListsViewModel: ToOurListsViewModel = (groupSlug) => (lists) => pipe(
  lists,
  RA.map(toListCardViewModel),
  (slimlineCards) => (slimlineCards.length > maxSlimlineCards
    ? truncatedView(slimlineCards, groupSlug)
    : { slimlineCards, viewAllListsUrl: O.none }
  ),
);
