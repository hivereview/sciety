import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { toOurListsViewModel } from '../../../src/group-page/about/to-our-lists-view-model';
import { arbitraryDate, arbitraryString } from '../../helpers';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryListOwnerId } from '../../types/list-owner-id.helper';

const arbitraryList = (name?: string) => ({
  listId: arbitraryListId(),
  name: name ?? arbitraryString(),
  description: arbitraryString(),
  articleIds: [],
  lastUpdated: arbitraryDate(),
  ownerId: arbitraryListOwnerId(),
});

describe('to-our-lists-view-model', () => {
  const groupSlug = arbitraryGroup().slug;

  describe('when the group has more than three lists', () => {
    const nameOfMostRecentlyUpdatedList = arbitraryString();
    const model = pipe(
      [
        arbitraryList(),
        arbitraryList(),
        arbitraryList(),
        arbitraryList(nameOfMostRecentlyUpdatedList),
      ],
      toOurListsViewModel(groupSlug),
    );

    it('returns list view models for only three lists', () => {
      expect(model.lists).toHaveLength(3);
    });

    it('returns list view models in reverse order', () => {
      expect(model.lists[0].title).toStrictEqual(nameOfMostRecentlyUpdatedList);
    });

    it('the View All Lists button is set', () => {
      expect(O.isSome(model.allListsUrl)).toBe(true);
    });

    it('the View All Lists button is a link to the lists tab', () => {
      expect(model.allListsUrl).toStrictEqual(O.some(`/groups/${groupSlug}/lists`));
    });
  });

  describe('when the group has two or three lists', () => {
    const nameOfMostRecentlyUpdatedList = arbitraryString();
    const model = pipe(
      [
        arbitraryList(),
        arbitraryList(),
        arbitraryList(nameOfMostRecentlyUpdatedList),
      ],
      toOurListsViewModel(groupSlug),
    );

    it('returns list view models for each list', () => {
      expect(model.lists).toHaveLength(3);
    });

    it('returns list view models in reverse order', () => {
      expect(model.lists[0].title).toStrictEqual(nameOfMostRecentlyUpdatedList);
    });

    it('the View All Lists button is not set', () => {
      expect(O.isNone(model.allListsUrl)).toBe(true);
    });
  });

  describe('when the group has one list', () => {
    const model = pipe(
      [
        arbitraryList(),
      ],
      toOurListsViewModel(groupSlug),
    );

    it('returns list view models for each list', () => {
      expect(model.lists).toHaveLength(1);
    });

    it('the View All Lists button is not set', () => {
      expect(O.isNone(model.allListsUrl)).toBe(true);
    });
  });

  describe('when the group has no lists', () => {
    it.todo('tbd');
  });
});
