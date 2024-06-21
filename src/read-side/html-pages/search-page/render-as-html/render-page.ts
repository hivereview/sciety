import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { HtmlFragment, toHtmlFragment } from '../../../../types/html-fragment';
import { renderListItems } from '../../shared-components/list-items';
import { renderSearchForm } from '../../shared-components/search-form';
import { ViewModel } from '../view-model';

const renderSearchCategories = (viewModel: ViewModel) => pipe(
  viewModel,
  RA.map((category) => toHtmlFragment(`<a href="${category.href}" class="search-categories__link">${category.title}</a>`)),
  renderListItems,
  (categories) => `
    <section>
    <h2>Browse by category</h2>
      <ul role="list" class="search-categories">
        ${categories}
      </ul>
    </section>
  `,
);

export const renderPage = (viewModel: ViewModel): HtmlFragment => pipe(
  `
    <header class="page-header page-header--search-results">
      <h1>Search Sciety</h1>
    </header>
    ${renderSearchForm('', true)}
    ${process.env.EXPERIMENT_ENABLED === 'true' ? renderSearchCategories(viewModel) : ''}
  `,
  toHtmlFragment,
);
