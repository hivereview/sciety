import { flow, pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { templateListItems } from '../../../shared-components/list-items';
import { ListId } from '../../../types/list-id';
import { toHtmlFragment } from '../../../types/html-fragment';

// ts-unused-exports:disable-next-line
export type ViewModel = ReadonlyArray<{
  listId: ListId,
  listName: string,
  listOwnerName: string,
}>;

const renderList = (listContent: string) => `
  <ul role="list">
    ${listContent}
  </ul> 
`;
export const renderListedIn = (viewModel: ViewModel) => toHtmlFragment(
  process.env.EXPERIMENT_ENABLED === 'true'
    ? pipe(
      viewModel,
      RA.map((item) => toHtmlFragment(`<a href="/lists/${item.listId}">${item.listName}${item.listOwnerName}</a>`)),
      RA.match(
        () => 'This article is not in any list yet, why not add it to one of your lists.',
        flow(
          templateListItems,
          renderList,
        ),
      ),
      (content) => `
      <div>
        <h2>Listed in</h2>        
        ${content}
      </div>
    `,
    )
    : '',
);
