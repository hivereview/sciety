import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { constant, pipe } from 'fp-ts/function';
import { ItemViewModel, renderSearchResult } from './render-search-result';
import { templateListItems } from '../shared-components';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

export type SearchResults = {
  query: string,
  itemsToDisplay: ReadonlyArray<ItemViewModel>,
  availableMatches: number,
};

const renderListIfNecessary = (articles: ReadonlyArray<HtmlFragment>) => pipe(
  articles,
  RNEA.fromReadonlyArray,
  O.fold(
    constant(''),
    (a) => `
      <ul class="search-results-list" role="list">
        ${templateListItems(a, 'search-results-list__item')}
      </ul>
    `,
  ),
);

type RenderSearchResults = (rs: SearchResults) => HtmlFragment;

export const renderSearchResults: RenderSearchResults = (searchResults) => pipe(
  searchResults.itemsToDisplay,
  RA.map(renderSearchResult),
  renderListIfNecessary,
  (searchResultsList) => `
    <p class="search-results__summary">
      Showing ${searchResults.itemsToDisplay.length} of ${searchResults.availableMatches} results for
      <span class="search-results__query">${htmlEscape(searchResults.query)}</span>
    </p>
    ${searchResultsList}
  `,
  toHtmlFragment,
);
