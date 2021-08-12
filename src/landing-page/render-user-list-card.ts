import * as O from 'fp-ts/Option';
import { templateDate } from '../shared-components/date';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

type UserListCardViewModel = {
  articleCount: number,
  lastUpdated: O.Option<Date>,
  handle: string,
};

const lastUpdated = O.fold(
  () => '',
  (date: Date) => `<span>Last updated ${templateDate(date)}</span>`,
);

export const renderUserListCard = (viewModel: UserListCardViewModel): HtmlFragment => toHtmlFragment(`
  <a href="/users/${viewModel.handle}/lists/saved-articles" class="list-card__link">
    <div class="list-card">
      <h3 class="list-card__title">@${viewModel.handle}</h3>
      <p>Saved articles</p>
      <div class="list-card__meta">
        <span class="visually-hidden">This list contains </span><span>${viewModel.articleCount} article${viewModel.articleCount === 1 ? '' : 's'}</span>${lastUpdated(viewModel.lastUpdated)}
      </div>
    </div>
  </a>
`);
