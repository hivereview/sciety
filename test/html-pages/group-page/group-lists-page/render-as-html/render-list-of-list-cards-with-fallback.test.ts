import * as O from 'fp-ts/Option';
import { renderListOfListCardsWithFallback } from '../../../../../src/html-pages/group-page/group-lists-page/render-as-html/render-list-of-list-cards-with-fallback';
import { arbitraryDate, arbitraryString } from '../../../../helpers';
import { arbitraryListId } from '../../../../types/list-id.helper';

describe('render-list-of-list-cards-with-fallback', () => {
  describe('when the group owns no lists', () => {
    const rendered = renderListOfListCardsWithFallback([]);

    it('displays a message instead of cards', () => {
      expect(rendered).toContain('This group doesn\'t have any lists yet.');
    });
  });

  describe('when the group has one empty list', () => {
    const listTitle = arbitraryString();
    const rendered = renderListOfListCardsWithFallback([
      {
        title: listTitle,
        listId: arbitraryListId(),
        description: arbitraryString(),
        articleCount: 0,
        lastUpdated: O.some(arbitraryDate()),
        articleCountLabel: arbitraryString(),
      },
    ]);

    it('displays a card for the list', () => {
      expect(rendered).toContain(listTitle);
    });
  });
});
