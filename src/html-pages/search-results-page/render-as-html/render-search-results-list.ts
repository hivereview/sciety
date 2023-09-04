import * as O from 'fp-ts/Option';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { pipe } from 'fp-ts/function';
import { renderListItems } from '../../../shared-components/render-list-items';
import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';

export const renderSearchResultsList = (cards: ReadonlyArray<HtmlFragment>): O.Option<HtmlFragment> => pipe(
  cards,
  RNEA.fromReadonlyArray,
  O.map(
    (a) => `
      <ol class="card-list" role="list">
        ${renderListItems(a)}
      </ol>
    `,
  ),
  O.map(toHtmlFragment),
);
