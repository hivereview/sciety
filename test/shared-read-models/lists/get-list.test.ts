import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import {
  articleAddedToList, listCreated, listDescriptionEdited, listNameEdited,
} from '../../../src/domain-events';
import { handleEvent, initialState } from '../../../src/shared-read-models/lists';
import { getList } from '../../../src/shared-read-models/lists/get-list';
import { arbitraryDate, arbitraryString } from '../../helpers';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryListOwnerId } from '../../types/list-owner-id.helper';

describe('get-list', () => {
  const listId = arbitraryListId();

  describe('when the list exists', () => {
    describe('and contains articles', () => {
      const name = arbitraryString();
      const description = arbitraryString();
      const articleId1 = arbitraryArticleId();
      const articleId2 = arbitraryArticleId();
      const readModel = pipe(
        [
          listCreated(listId, name, description, arbitraryListOwnerId()),
          articleAddedToList(articleId1, listId, new Date('2019')),
          articleAddedToList(articleId2, listId, new Date('2021')),
        ],
        RA.reduce(initialState(), handleEvent),
      );

      it('returns the list name', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          name,
        })));
      });

      it('returns the list description', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          description,
        })));
      });

      it('returns the articleIds, sorted by date added, descending', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          articleIds: [articleId2.value, articleId1.value],
        })));
      });
    });

    describe('and is empty', () => {
      const name = arbitraryString();
      const description = arbitraryString();
      const readModel = pipe(
        [
          listCreated(listId, name, description, arbitraryListOwnerId()),
        ],
        RA.reduce(initialState(), handleEvent),
      );

      it('returns articleIds as empty', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          articleIds: [],
        })));
      });

      it('returns the lists name', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          name,
        })));
      });

      it('returns the list description', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          description,
        })));
      });
    });

    describe('and has had its name edited', () => {
      const name = arbitraryString();
      const description = arbitraryString();
      const dateOfLatestEvent = arbitraryDate();
      const readModel = pipe(
        [
          listCreated(listId, arbitraryString(), description, arbitraryListOwnerId()),
          listNameEdited(listId, name, dateOfLatestEvent),
        ],
        RA.reduce(initialState(), handleEvent),
      );

      it('returns the latest name', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          name,
        })));
      });

      it('returns the same list description', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          description,
        })));
      });

      it('returns the date of the latest event as the lastUpdated', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          lastUpdated: dateOfLatestEvent,
        })));
      });
    });

    describe('and has had its description edited', () => {
      const name = arbitraryString();
      const description = arbitraryString();
      const dateOfLatestEvent = arbitraryDate();
      const readModel = pipe(
        [
          listCreated(listId, name, arbitraryString(), arbitraryListOwnerId()),
          listDescriptionEdited(listId, description, dateOfLatestEvent),
        ],
        RA.reduce(initialState(), handleEvent),
      );

      it('returns the same list name', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          name,
        })));
      });

      it.failing('returns the latest description', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          description,
        })));
      });

      it.failing('returns the date of the latest event as the lastUpdated', () => {
        expect(getList(readModel)(listId)).toStrictEqual(O.some(expect.objectContaining({
          lastUpdated: dateOfLatestEvent,
        })));
      });
    });
  });

  describe('when the list does not exist', () => {
    const readModel = pipe(
      [],
      RA.reduce(initialState(), handleEvent),
    );

    it('returns not found', () => {
      expect(getList(readModel)(listId)).toStrictEqual(O.none);
    });
  });
});
