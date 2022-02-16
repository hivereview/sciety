import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { evaluationRecorded, listCreated } from '../../../src/domain-events';
import { List, selectAllListsOwnedBy } from '../../../src/shared-read-models/lists';
import { arbitraryDate, arbitraryString } from '../../helpers';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('select-all-lists-owned-by', () => {
  const ownerId = arbitraryGroupId();

  describe('when the group does not own any list', () => {
    let result: ReadonlyArray<List>;

    beforeEach(async () => {
      result = await pipe(
        [],
        selectAllListsOwnedBy(ownerId),
      )();
    });

    it('returns an empty array of lists', () => {
      expect(result).toStrictEqual([]);
    });
  });

  describe('when the group owns an empty list', () => {
    const listName = arbitraryString();
    const listDescription = arbitraryString();
    const listCreationDate = arbitraryDate();
    let result: List;

    beforeEach(async () => {
      result = await pipe(
        [
          listCreated(arbitraryListId(), listName, listDescription, ownerId, listCreationDate),
        ],
        selectAllListsOwnedBy(ownerId),
        T.map((lists) => lists[0]),
      )();
    });

    it('returns the list name', () => {
      expect(result.name).toBe(listName);
    });

    it('returns the list description', () => {
      expect(result.description).toBe(listDescription);
    });

    it('returns a count of 0', () => {
      expect(result.articleCount).toBe(0);
    });

    it('returns the list creation date as last updated date', () => {
      expect(result.lastUpdated).toStrictEqual(O.some(listCreationDate));
    });
  });

  describe('when the group owns a list whose content is determined by evaluation events', () => {
    const listName = 'Evaluated articles';
    const listDescription = arbitraryString();
    const newerDate = new Date('2021-07-08');
    let result: List;

    beforeEach(async () => {
      result = await pipe(
        [
          listCreated(arbitraryListId(), listName, listDescription, ownerId),
          evaluationRecorded(ownerId, arbitraryDoi(), arbitraryReviewId()),
          evaluationRecorded(ownerId, arbitraryDoi(), arbitraryReviewId(), [], new Date(), newerDate),
        ],
        selectAllListsOwnedBy(ownerId),
        T.map((lists) => lists[0]),
      )();
    });

    it('returns the list name', () => {
      expect(result.name).toBe(listName);
    });

    it('returns the list description', () => {
      expect(result.description).toBe(listDescription);
    });

    it('returns a count of the articles', () => {
      expect(result.articleCount).toBe(2);
    });

    it('returns the last updated date', () => {
      expect(result.lastUpdated).toStrictEqual(O.some(newerDate));
    });
  });

  describe('when the group owns a list whose content is determined by evaluation events and the group has evaluated one article more than once', () => {
    const olderDate = new Date('2021-07-08');
    const articleId = arbitraryDoi();
    let result: List;

    beforeEach(async () => {
      result = await pipe(
        [
          listCreated(arbitraryListId(), 'Evaluated articles', arbitraryString(), ownerId),
          evaluationRecorded(ownerId, articleId, arbitraryReviewId(), [], new Date(), olderDate),
          evaluationRecorded(ownerId, articleId, arbitraryReviewId()),
        ],
        selectAllListsOwnedBy(ownerId),
        T.map((lists) => lists[0]),
      )();
    });

    it('returns a count of 1', () => {
      expect(result.articleCount).toBe(1);
    });

    it.skip('returns the date of the article first being added to the list', () => {
      expect(result.lastUpdated).toStrictEqual(O.some(olderDate));
    });
  });

  describe('when a list with a different owner contains some articles', () => {
    const anotherOwnerId = arbitraryGroupId();
    let result: ReadonlyArray<List>;

    beforeEach(async () => {
      result = await pipe(
        [
          listCreated(arbitraryListId(), 'Evaluated articles', arbitraryString(), anotherOwnerId),
          evaluationRecorded(anotherOwnerId, arbitraryDoi(), arbitraryReviewId()),
        ],
        selectAllListsOwnedBy(ownerId),
      )();
    });

    it('returns an empty array of lists', () => {
      expect(result).toStrictEqual([]);
    });
  });
});
