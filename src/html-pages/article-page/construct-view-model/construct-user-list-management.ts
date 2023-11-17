import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { ArticleId } from '../../../types/article-id.js';
import { UserId } from '../../../types/user-id.js';
import { ViewModel } from '../view-model.js';
import { Dependencies } from './dependencies.js';

export const constructUserListManagement = (user: O.Option<{ id: UserId }>, dependencies: Dependencies, articleId: ArticleId): ViewModel['userListManagement'] => pipe(
  user,
  O.map(
    ({ id }) => pipe(
      dependencies.selectListContainingArticle(id)(articleId),
      O.foldW(
        () => E.left({
          saveArticleHref: `/save-article?articleId=${articleId.value}`,
        }),
        (list) => E.right({
          listId: list.id,
          listName: list.name,
          listHref: `/lists/${list.id}`,
        }),
      ),
    ),
  ),
);
