import * as TE from 'fp-ts/TaskEither';
import { toHtmlFragment } from '../../../types/html-fragment';
import { ErrorPageViewModel } from '../construct-error-page-view-model';
import { HtmlPage, toHtmlPage } from '../html-page';
import { renderSearchForm } from '../shared-components/search-form';

export const searchPage: TE.TaskEither<ErrorPageViewModel, HtmlPage> = TE.right(toHtmlPage({
  title: 'Search',
  content: toHtmlFragment(`
    <header class="page-header page-header--search-results">
      <h1>Search Sciety</h1>
    </header>
    ${renderSearchForm('', true)}
  `),
}));
