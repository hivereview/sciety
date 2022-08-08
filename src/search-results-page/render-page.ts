import * as TE from 'fp-ts/TaskEither';
import { renderSearchResults, SearchResults } from './render-search-results';
import { renderSearchForm } from '../shared-components/render-search-form';
import * as DE from '../types/data-error';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';

export type RenderPage = (query: string) => TE.TaskEither<RenderPageError, Page>;

export const renderErrorPage = (error: DE.DataError): RenderPageError => ({
  type: error,
  message: toHtmlFragment('We\'re having trouble accessing search right now, please try again later.'),
});

type SearchParams = {
  query: string,
  evaluatedOnly: boolean,
};

export const renderSearchResultsHeader = (searchParams: SearchParams): Page => ({
  title: `Search results for ${searchParams.query}`,
  content: toHtmlFragment(`
    <header class="page-header page-header--search-results">
      <h1>Search Sciety</h1>
    </header>
    ${renderSearchForm(searchParams.query, searchParams.evaluatedOnly)}
    <section class="search-results">`),
});

export const renderPage = (searchResults: SearchResults): Page => ({
  title: `Search results for ${searchResults.query}`,
  content: toHtmlFragment(`
      ${renderSearchResults(searchResults)}
    </section>
  `),
});
