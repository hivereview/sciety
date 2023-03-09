import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import * as RA from 'fp-ts/ReadonlyArray';
import { ListsTab, ViewModel as UserListsPage } from '../../src/html-pages/user-page/view-model';
import { constructViewModel as constructUserListsPage } from '../../src/html-pages/user-page/construct-view-model';
import * as LOID from '../../src/types/list-owner-id';
import {
  constructViewModel as constructGroupFollowersPage,
} from '../../src/html-pages/group-page/group-followers-page/construct-view-model/construct-view-model'; import { ViewModel as GroupFollowersPage } from '../../src/html-pages/group-page/group-followers-page/view-model';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryGroup } from '../types/group.helper';
import { arbitraryUserDetails } from '../types/user-details.helper';
import { arbitraryList } from '../types/list-helper';
import { CandidateUserHandle } from '../../src/types/candidate-user-handle';
import { createReadAndWriteSides, ReadAndWriteSides } from '../create-read-and-write-sides';
import { CommandHelpers, createCommandHelpers } from '../create-command-helpers';

describe('create user list', () => {
  let commandHandlers: ReadAndWriteSides['commandHandlers'];
  let getAllEvents: ReadAndWriteSides['getAllEvents'];
  let queries: ReadAndWriteSides['queries'];
  let commandHelpers: CommandHelpers;

  beforeEach(() => {
    ({ queries, getAllEvents, commandHandlers } = createReadAndWriteSides());
    commandHelpers = createCommandHelpers(commandHandlers);
  });

  describe('given a user who is following a group', () => {
    const user = arbitraryUserDetails();
    const group = arbitraryGroup();

    beforeEach(async () => {
      await commandHelpers.createUserAccount(user);
      await commandHelpers.createGroup(group);
      await commandHelpers.followGroup(user.id, group.id);
    });

    describe('when the user creates a new list', () => {
      const list = {
        ...arbitraryList(),
        ownerId: LOID.fromUserId(user.id),
      };

      beforeEach(async () => {
        await commandHelpers.createList(list);
      });

      describe('on the user-lists page', () => {
        let userListsPage: UserListsPage;

        beforeEach(async () => {
          userListsPage = await pipe(
            {
              handle: user.handle as string as CandidateUserHandle,
              user: O.none,
            },
            constructUserListsPage('lists', { ...queries, getAllEvents }),
            TE.getOrElse(shouldNotBeCalled),
          )();
        });

        it('the tabs count the list', () => {
          expect(userListsPage.listCount).toBe(2);
        });

        it('there is a card for the list', () => {
          const listIds = pipe(
            userListsPage.activeTab,
            O.fromPredicate((tab): tab is ListsTab => tab.selector === 'lists'),
            O.getOrElseW(shouldNotBeCalled),
            (tab) => tab.ownedLists,
            RA.map((l) => l.listId),
          );

          expect(listIds).toContain(list.id);
        });
      });

      describe('on the group-followers page', () => {
        let groupFollowersPage: GroupFollowersPage;

        beforeEach(async () => {
          groupFollowersPage = await pipe(
            {
              slug: group.slug,
              user: O.none,
              page: 1,
            },
            constructGroupFollowersPage(queries),
            TE.getOrElse(shouldNotBeCalled),
          )();
        });

        it.failing('the user card counts the extra list', () => {
          expect(groupFollowersPage.followers[0].listCount).toBe(2);
        });
      });
    });
  });
});
