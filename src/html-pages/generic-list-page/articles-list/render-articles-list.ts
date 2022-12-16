import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow, pipe } from 'fp-ts/function';
import { ArticleErrorCardViewModel, renderArticleErrorCard } from './render-article-error-card';
import { ArticleViewModel, renderArticleCardWithControlsAndOptionalAnnotation } from '../../../shared-components/article-card';
import { Doi } from '../../../types/doi';
import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';
import { ListId } from '../../../types/list-id';

export type ArticleCardWithControlsViewModel = {
  articleViewModel: ArticleViewModel,
  controls: O.Option<{ articleId: Doi, listId: ListId }>,
  annotationContent?: HtmlFragment,
};

type RenderArticlesList = (
  articleViewModels: ReadonlyArray<E.Either<ArticleErrorCardViewModel, ArticleCardWithControlsViewModel>>,
) => HtmlFragment;

const renderRemoveArticleForm = (articleId: Doi, listId: ListId) => pipe(
  articleId.value,
  (id) => `<form method="post" action="/forms/remove-article-from-list">
      <input type="hidden" name="articleid" value="${id}">
      <input type="hidden" name="listid" value="${listId}">
      <button aria-label="Remove this article from the list" class="saved-articles-control">
        <svg xmlns="http://www.w3.org/2000/svg" height="24px" viewBox="0 0 24 24" width="24px" class="saved-articles-control__icon">
          <desc>Remove this article from the list</desc>
          <path d="M0 0h24v24H0V0z" fill="none"/>
          <path d="M16 9v10H8V9h8m-1.5-6h-5l-1 1H5v2h14V4h-3.5l-1-1zM18 7H6v12c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7z"/>
        </svg>
      </button>
    </form>`,
  toHtmlFragment,
);

export const renderArticlesList: RenderArticlesList = flow(
  RA.map(E.fold(
    renderArticleErrorCard,
    (viewModel) => renderArticleCardWithControlsAndOptionalAnnotation(
      pipe(
        viewModel.controls,
        O.map(({ articleId, listId }) => renderRemoveArticleForm(articleId, listId)),
      ),
      viewModel.annotationContent,
    )(viewModel.articleViewModel),
  )),
  RA.map((activity) => `<li class="articles-list__item">${activity}</li>`),
  (renderedActivities) => `
      <ul class="articles-list" role="list">${renderedActivities.join('')}</ul>
  `,
  toHtmlFragment,
);
