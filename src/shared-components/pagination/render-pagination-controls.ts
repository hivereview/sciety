import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

type ViewModel = {
  nextPageHref: O.Option<string>,
};

export const renderPaginationControls = (viewModel: ViewModel): HtmlFragment => pipe(
  viewModel.nextPageHref,
  O.fold(
    () => '',
    (url) => `<div class="pagination-controls">
      <a href="${url}" class="pagination-controls__next_link">Next</a>
    </div>`,
  ),
  toHtmlFragment,
);
