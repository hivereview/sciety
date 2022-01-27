import { pipe } from 'fp-ts/function';
import { articleAddedToList } from '../../src/domain-events/article-added-to-list-event';
import { needsToBeAdded } from '../../src/infrastructure/needs-to-be-added';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryListId } from '../types/list-id.helper';

describe('needs-to-be-added', () => {
  const articleId = arbitraryDoi();
  const listId = arbitraryListId();
  const eventToAdd = articleAddedToList(articleId, listId);

  describe('when the event to be added is an existing event', () => {
    const existingEvents = [
      articleAddedToList(articleId, listId),
    ];
    const result = pipe(
      eventToAdd,
      needsToBeAdded(existingEvents),
    );

    it('returns false', () => {
      expect(result).toBe(false);
    });
  });

  describe('when the event to be added is not an existing event', () => {
    const result = pipe(
      eventToAdd,
      needsToBeAdded([]),
    );

    it.skip('returns true', () => {
      expect(result).toBe(true);
    });
  });
});
