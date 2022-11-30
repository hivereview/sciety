import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { articleRemovedFromList } from '../../../src/domain-events';
import { articleAddedToList } from '../../../src/domain-events/article-added-to-list-event';
import { listCreated } from '../../../src/domain-events/list-created-event';
import { handleEvent, initialState, isArticleOnTheListOwnedBy } from '../../../src/shared-read-models/lists';
import * as LOID from '../../../src/types/list-owner-id';
import { arbitraryString } from '../../helpers';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryUserId } from '../../types/user-id.helper';

describe('is-article-on-the-list-owned-by', () => {
  const articleId = arbitraryArticleId();
  const listId = arbitraryListId();
  const userId = arbitraryUserId();

  describe('when the user has added an article to the list', () => {
    const readModel = pipe(
      [
        listCreated(listId, arbitraryString(), arbitraryString(), LOID.fromUserId(userId)),
        articleAddedToList(articleId, listId),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('the query returns the first list id', () => {
      expect(isArticleOnTheListOwnedBy(readModel)(userId)(articleId)).toStrictEqual(O.some(listId));
    });
  });

  describe('when the user has added and removed an article', () => {
    const readModel = pipe(
      [
        listCreated(listId, arbitraryString(), arbitraryString(), LOID.fromUserId(userId)),
        articleAddedToList(articleId, listId),
        articleRemovedFromList(articleId, listId),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('the query returns nothing', () => {
      expect(isArticleOnTheListOwnedBy(readModel)(userId)(articleId)).toStrictEqual(O.none);
    });
  });

  describe('when two users have added articles', () => {
    const userId2 = arbitraryUserId();
    const listId2 = arbitraryListId();
    const readModel = pipe(
      [
        listCreated(listId, arbitraryString(), arbitraryString(), LOID.fromUserId(userId)),
        articleAddedToList(articleId, listId),
        listCreated(listId2, arbitraryString(), arbitraryString(), LOID.fromUserId(userId2)),
        articleAddedToList(articleId, listId2),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('the query returns the first list id belonging to the first user', () => {
      expect(isArticleOnTheListOwnedBy(readModel)(userId)(articleId)).toStrictEqual(O.some(listId));
    });

    it('the query returns the first list id belonging to the second user', () => {
      expect(isArticleOnTheListOwnedBy(readModel)(userId2)(articleId)).toStrictEqual(O.some(listId2));
    });
  });
});
