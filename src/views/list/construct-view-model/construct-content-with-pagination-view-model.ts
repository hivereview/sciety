import * as T from 'fp-ts/Task';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { toPageOfCards } from './to-page-of-cards.js';
import { ArticleId } from '../../../types/article-id.js';
import { ViewModel } from '../view-model.js';
import { ListId } from '../../../types/list-id.js';
import { Dependencies } from './dependencies.js';

export const constructContentWithPaginationViewModel = (
  dependencies: Dependencies,
  listId: ListId,
) => (articleIds: ReadonlyArray<ArticleId>): T.Task<ViewModel['articles']> => pipe(
  articleIds,
  RA.takeLeft(20),
  RA.map(dependencies.getActivityForDoi),
  toPageOfCards(dependencies, listId),
);
