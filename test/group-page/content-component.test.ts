import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { listCreated, userFollowedEditorialCommunity } from '../../src/domain-events';
import { contentComponent } from '../../src/group-page/content-component';
import { arbitraryString } from '../helpers';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryGroup } from '../types/group.helper';
import { arbitraryListId } from '../types/list-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('content-component', () => {
  const group = arbitraryGroup();

  describe('the followers tab', () => {
    const events = [
      userFollowedEditorialCommunity(arbitraryUserId(), group.id),
      userFollowedEditorialCommunity(arbitraryUserId(), group.id),
    ];
    const ports = {
      fetchStaticFile: () => TE.right(''),
      getUserDetailsBatch: () => TE.right([]),
      getAllEvents: T.of(events),
      getGroup: () => TE.right(group),
    };

    it.each([
      [0 as const],
      [1 as const],
      [2 as const],
    ])('when loading tab index %d displays the followers count on the followers tab label', async (activeTabIndex) => {
      const content = await pipe(
        contentComponent(ports)(group, 1, activeTabIndex),
        TE.getOrElse(shouldNotBeCalled),
        T.map(JSDOM.fragment),
      )();
      const followersTabLabel = content.querySelector('.tab:nth-child(3)')?.textContent;

      expect(followersTabLabel).toContain('(2)');
    });
  });

  describe('the lists tab', () => {
    const listCreatedEvents = [
      listCreated(arbitraryListId(), arbitraryString(), arbitraryString(), group.id),
      listCreated(arbitraryListId(), arbitraryString(), arbitraryString(), group.id),
    ];
    const ports = {
      fetchStaticFile: () => TE.right(''),
      getUserDetailsBatch: () => TE.right([]),
      getAllEvents: T.of(listCreatedEvents),
      getGroup: () => TE.right(group),
    };

    it.skip('when loading tab index 1 displays the lists count on the lists tab label', async () => {
      const content = await pipe(
        contentComponent(ports)(group, 1, 1),
        TE.getOrElse(shouldNotBeCalled),
        T.map(JSDOM.fragment),
      )();
      const followersTabLabel = content.querySelector('.tab:first-child')?.textContent;

      expect(followersTabLabel).toContain(`(${listCreatedEvents.length})`);
    });
  });
});
