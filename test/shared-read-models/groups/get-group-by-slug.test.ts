import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { arbitraryUninterestingEvents } from './arbitrary-uninteresting-events.helper';
import { groupJoined } from '../../../src/domain-events';
import { handleEvent, initialState } from '../../../src/shared-read-models/groups';
import { getGroupBySlug } from '../../../src/shared-read-models/groups/get-group-by-slug';
import { arbitraryGroup } from '../../types/group.helper';

const group = arbitraryGroup();

describe('getGroupBySlug', () => {
  describe('when the group exists', () => {
    const readmodel = pipe(
      [
        ...arbitraryUninterestingEvents,
        groupJoined(group),
        ...arbitraryUninterestingEvents,
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('returns the group', () => {
      expect(getGroupBySlug(readmodel)(group.slug)).toStrictEqual(O.some(group));
    });
  });

  describe('when the group does not exist', () => {
    const readmodel = pipe(
      [
        ...arbitraryUninterestingEvents,
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it('returns not-found', () => {
      expect(getGroupBySlug(readmodel)(group.id)).toStrictEqual(O.none);
    });
  });
});
