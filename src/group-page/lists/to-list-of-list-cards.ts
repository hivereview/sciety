import * as RA from 'fp-ts/ReadonlyArray';
import { flow, pipe } from 'fp-ts/function';
import { toListCardViewModel } from './to-list-card-view-model';
import { renderListCard } from '../../shared-components/list-card/render-list-card';
import { templateListItems } from '../../shared-components/list-items';
import { List } from '../../shared-read-models/lists';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

const renderCards = (cards: ReadonlyArray<HtmlFragment>) => pipe(
  cards,
  (items) => templateListItems(items, 'group-page-followers-list__item'),
  (listContent) => `
    <section class="group-page-lists">
      <ul class="group-page-followers-list" role="list">
        ${listContent}
      </ul>
    </section>
  `,
  toHtmlFragment,
);

type ToListOfListCards = (lists: ReadonlyArray<List>)
=> HtmlFragment;

export const toListOfListCards: ToListOfListCards = (lists) => pipe(
  lists,
  RA.map(toListCardViewModel),
  RA.match(
    () => toHtmlFragment('<p class="static-message">This group doesn\'t have any lists yet.</p>'),
    flow(
      RA.map(renderListCard),
      renderCards,
    ),
  ),
);
