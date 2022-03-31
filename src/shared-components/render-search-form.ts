import { htmlEscape } from 'escape-goat';
import { renderExampleSearches } from './render-example-searches';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

export const renderSearchForm = (query: string, evaluatedOnly: boolean): HtmlFragment => toHtmlFragment(`
  <div class="search-form">
    <form action="/search" method="get" class="search-form__form">
      <input type="hidden" name="category" value="articles">
      <label for="searchText" class="visually-hidden">Search term</label>
      ${htmlEscape`<input value="${query}" id="searchText" name="query" placeholder="Find articles and evaluating groups…" class="search-form__text">`}
      <label for="searchEvaluatedOnlyFilter" class="search-form__label">Search only evaluated articles</label>
      <input type="checkbox" name="evaluatedOnly" value="true" id="searchEvaluatedOnlyFilter"${evaluatedOnly ? ' checked' : ''}>
      <button type="reset" id="clearSearchText" class="search-form__clear visually-hidden">
        <img src="/static/images/clear-search-text-icon.svg" class="search-form__clear_icon" alt="">
      </button>
      <button type="submit" class="search-form__submit">Search</button>
    </form>
    ${renderExampleSearches()}
  </div>
`);
