import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { ViewModel } from '../view-model';
import { toHtmlFragment } from '../../../types/html-fragment';
import { renderListCard } from '../../../shared-components/list-card/render-list-card';
import { templateListItems } from '../../../shared-components/list-items';

export const renderPage = (viewModel: ViewModel) => pipe(
  viewModel,
  RA.map(renderListCard),
  templateListItems,
  (listCards) => `<ol role="list">${listCards}</ol>`,
  toHtmlFragment,
);
