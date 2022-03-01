import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { articleAddedToList, listCreated } from '../../../src/domain-events';
import { getList, List } from '../../../src/shared-read-models/lists';
import { arbitraryDate, arbitraryString } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryListId } from '../../types/list-id.helper';

describe('get-list', () => {
  describe('when the listId does not exist', () => {
    it.todo('returns not-found');
  });

  describe('when the listId does exist', () => {
    describe('and it refers to a hardcoded list', () => {
      describe('when the list is empty', () => {
        it.todo('returns the correct List');

        it.todo('with the correct last updated date');
      });

      describe('when the list is non-empty', () => {
        const latestDate = arbitraryDate();
        const eLifeMedicineListId = 'c7237468-aac1-4132-9598-06e9ed68f31d';
        let result: List;

        beforeEach(async () => {
          result = await pipe(
            [
              articleAddedToList(arbitraryDoi(), eLifeMedicineListId, arbitraryDate()),
              articleAddedToList(arbitraryDoi(), eLifeMedicineListId, latestDate),
            ],
            getList(eLifeMedicineListId),
            TE.getOrElse(shouldNotBeCalled),
          )();
        });

        it.todo('returns the correct List');

        it('with the correct last updated date', () => {
          expect(result.lastUpdated).toStrictEqual(latestDate);
        });
      });
    });

    describe('and it refers to a non-hardcoded list', () => {
      const listId = arbitraryListId();

      describe('when the list is empty', () => {
        const creationDate = arbitraryDate();
        let result: List;

        beforeEach(async () => {
          result = await pipe(
            [
              listCreated(listId, arbitraryString(), arbitraryString(), arbitraryGroupId(), creationDate),
            ],
            getList(listId),
            TE.getOrElse(shouldNotBeCalled),
          )();
        });

        it.todo('returns non dynamic metadata sourced from list creation event');

        it('returns the list creation date as the last updated date', () => {
          expect(result.lastUpdated).toStrictEqual(creationDate);
        });

        it.todo('returns an articleCount of 0');
      });

      describe('when the list is non-empty', () => {
        const name = arbitraryString();
        const description = arbitraryString();
        const latestDate = arbitraryDate();
        const ownerId = arbitraryGroupId();
        let result: List;

        beforeEach(async () => {
          result = await pipe(
            [
              listCreated(listId, name, description, ownerId),
              articleAddedToList(arbitraryDoi(), listId, arbitraryDate()),
              articleAddedToList(arbitraryDoi(), listId, latestDate),
            ],
            getList(listId),
            TE.getOrElse(shouldNotBeCalled),
          )();
        });

        it('returns non-dynamic metadata sourced from list creation event', () => {
          expect(result).toStrictEqual(expect.objectContaining({
            name,
            description,
            ownerId,
          }));
        });

        it('returns date of last addition to list as the last updated date', () => {
          expect(result.lastUpdated).toStrictEqual(latestDate);
        });

        it('returns an articleCount of 2', () => {
          expect(result.articleCount).toBe(2);
        });
      });
    });
  });
});
