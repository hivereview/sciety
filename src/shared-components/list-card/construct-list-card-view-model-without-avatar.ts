import * as O from 'fp-ts/Option';
import { List } from '../../types/list';
import { ListCardViewModel } from './render-list-card';

export const constructListCardViewModelWithoutAvatar = (list: List): ListCardViewModel => ({
  listId: list.id,
  articleCount: list.articleIds.length,
  updatedAt: O.some(list.updatedAt),
  title: list.name,
  description: list.description,
  avatarUrl: O.none,
});
