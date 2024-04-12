import * as O from 'fp-ts/Option';
import { ListId } from '../../../types/list-id';
import { HeaderViewModel } from '../sub-page-header';

type ListViewModel = {
  listId: ListId,
  articleCount: number,
  updatedAt: Date,
  title: string,
  listHref: string,
};

type OurListsViewModel = {
  lists: ReadonlyArray<ListViewModel>,
  allListsUrl: O.Option<string>,
};

export type ViewModel = {
  ourLists: OurListsViewModel,
  markdown: string,
  header: HeaderViewModel,
};
