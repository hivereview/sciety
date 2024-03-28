import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { ListId } from '../../types/list-id';
import { templateDate } from '../date';
import { renderListPageLinkHref } from '../render-list-page-link-href';
import { renderCountWithDescriptor } from '../render-count-with-descriptor';
import { RawUserInput } from '../../read-side';
import { safelyRenderRawUserInput } from '../raw-user-input-renderers';

export type ListCardViewModel = {
  listId: ListId,
  articleCount: number,
  updatedAt: O.Option<Date>,
  title: string,
  description: RawUserInput,
  avatarUrl: O.Option<string>,
  curatedByUser: boolean,
  ownerDisplayName: string,
};

const lastUpdated = O.fold(
  () => '',
  (date: Date) => `<span>Last updated ${templateDate(date)}</span>`,
);

export const renderAvatar = O.fold(
  () => '',
  (avatarUrl: string) => `<img class="list-card__avatar" src="${avatarUrl}" alt="" />`,
);

const renderListCardWithoutCurator = (viewModel: ListCardViewModel): HtmlFragment => toHtmlFragment(`
  <article class="list-card">
    <div class="list-card__body">
      <div>
        <h3 class="list-card__title"><a href="${renderListPageLinkHref(viewModel.listId)}" class="list-card__link">${htmlEscape(viewModel.title)}</a></h3>
        <p>${safelyRenderRawUserInput(viewModel.description)}</p>
      </div>
      <div class="list-card__meta">
        <span class="visually-hidden">This list contains </span><span>${renderCountWithDescriptor(viewModel.articleCount, 'article', 'articles')}</span>${lastUpdated(viewModel.updatedAt)}
      </div>
    </div>
    ${renderAvatar(viewModel.avatarUrl)}
  </article>
`);

const renderListCardWithCurator = (viewModel: ListCardViewModel): HtmlFragment => toHtmlFragment(`
  <article class="list-card">
    <div class="list-card__body">
      <div>
        <h3 class="list-card__title"><a href="${renderListPageLinkHref(viewModel.listId)}" class="list-card__link">${htmlEscape(viewModel.title)}</a></h3>
        <p>${safelyRenderRawUserInput(viewModel.description)}</p>
      </div>
      <div class="list-card__curator">
        ${renderAvatar(viewModel.avatarUrl)}<span>Curated by ${viewModel.ownerDisplayName}</span>
      </div>
      <div class="list-card__meta">
        <span class="visually-hidden">This list contains </span><span>${renderCountWithDescriptor(viewModel.articleCount, 'article', 'articles')}</span>${lastUpdated(viewModel.updatedAt)}
      </div>
    </div>
  </article>
`);

export const renderListCard = (viewModel: ListCardViewModel): HtmlFragment => (
  (process.env.EXPERIMENT_ENABLED === 'true' && viewModel.curatedByUser)
    ? renderListCardWithCurator(viewModel)
    : renderListCardWithoutCurator(viewModel)
);
