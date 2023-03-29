import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { UserDetails } from '../../../../src/types/user-details';
import { arbitraryArticleId } from '../../../types/article-id.helper';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { constructViewModel, Ports } from '../../../../src/html-pages/article-page/construct-view-model';
import * as LOID from '../../../../src/types/list-owner-id';
import { arbitraryUserDetails } from '../../../types/user-details.helper';
import { List } from '../../../../src/types/list';
import { arbitraryList } from '../../../types/list-helper';
import { createTestFramework, TestFramework } from '../../../framework';
import {
  LoggedInUserListManagement,
} from '../../../../src/html-pages/article-page/view-model';

describe('construct-view-model', () => {
  let framework: TestFramework;
  let adapters: Ports;
  const articleId = arbitraryArticleId();

  beforeEach(() => {
    framework = createTestFramework();
    adapters = {
      ...framework.queries,
      ...framework.happyPathThirdParties,
      getAllEvents: framework.getAllEvents,
    };
  });

  describe('when the user is logged in', () => {
    let userDetails: UserDetails;

    beforeEach(async () => {
      userDetails = arbitraryUserDetails();
      await framework.commandHelpers.createUserAccount(userDetails);
    });

    describe('when the article is not saved to the user\'s only list', () => {
      let list: List;
      let viewModel: LoggedInUserListManagement;

      beforeEach(async () => {
        // eslint-disable-next-line prefer-destructuring
        list = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(userDetails.id))[0];
        viewModel = await pipe(
          {
            doi: articleId,
            user: O.some({ id: userDetails.id }),
          },
          constructViewModel(adapters),
          TE.map((v) => v.userListManagement),
          TE.map(O.getOrElseW(shouldNotBeCalled)),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it('list management has access to the default user list id', () => {
        expect(viewModel).toStrictEqual(E.left(expect.objectContaining({
          lists: [expect.objectContaining({ listId: list.id })],
        })));
      });

      it('list management has access to the default user list name', () => {
        expect(viewModel).toStrictEqual(E.left(expect.objectContaining({
          lists: [expect.objectContaining({ listName: list.name })],
        })));
      });
    });

    describe('when the article is not saved to any of the user\'s multiple lists', () => {
      let list: List;
      let viewModel: LoggedInUserListManagement;
      let usersLists: ReadonlyArray<List>;

      beforeEach(async () => {
        list = arbitraryList(LOID.fromUserId(userDetails.id));
        await framework.commandHelpers.createList(list);
        usersLists = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(userDetails.id));
        viewModel = await pipe(
          {
            doi: articleId,
            user: O.some({ id: userDetails.id }),
          },
          constructViewModel(adapters),
          TE.map((v) => v.userListManagement),
          TE.map(O.getOrElseW(shouldNotBeCalled)),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it.skip('the user\'s lists are ordered by descending of lastUpdated', () => {
        expect(viewModel).toStrictEqual(E.left({
          lists: [
            expect.objectContaining({ listId: usersLists[1].id }),
            expect.objectContaining({ listId: usersLists[0].id }),
          ],
        }));
      });

      it.skip('list management has access to all of the user\'s multiple lists', () => {
        expect(viewModel).toStrictEqual(E.left({
          lists: [
            { listId: usersLists[1].id, listName: usersLists[1].name },
            { listId: usersLists[0].id, listName: usersLists[0].name },
          ],
        }));
      });
    });

    describe('when the article is saved to the default user list', () => {
      let list: List;
      let viewModel: LoggedInUserListManagement;

      beforeEach(async () => {
        // eslint-disable-next-line prefer-destructuring
        list = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(userDetails.id))[0];
        await framework.commandHelpers.addArticleToList(articleId, list.id);
        viewModel = await pipe(
          {
            doi: articleId,
            user: O.some({ id: userDetails.id }),
          },
          constructViewModel(adapters),
          TE.map((v) => v.userListManagement),
          TE.map(O.getOrElseW(shouldNotBeCalled)),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it('list management has access to list id', async () => {
        expect(viewModel).toStrictEqual(E.right(expect.objectContaining({
          listId: list.id,
        })));
      });

      it('list management has access to list name', () => {
        expect(viewModel).toStrictEqual(E.right(expect.objectContaining({
          listName: list.name,
        })));
      });
    });

    describe('when the article is saved to another user list', () => {
      let list: List;
      let viewModel: LoggedInUserListManagement;

      beforeEach(async () => {
        list = arbitraryList(LOID.fromUserId(userDetails.id));
        await framework.commandHelpers.createList(list);
        await framework.commandHelpers.addArticleToList(articleId, list.id);
        viewModel = await pipe(
          {
            doi: articleId,
            user: O.some({ id: userDetails.id }),
          },
          constructViewModel(adapters),
          TE.map((v) => v.userListManagement),
          TE.map(O.getOrElseW(shouldNotBeCalled)),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it('list management has access to list id', () => {
        expect(viewModel).toStrictEqual(E.right(expect.objectContaining({
          listId: list.id,
        })));
      });

      it('list management has access to list name', () => {
        expect(viewModel).toStrictEqual(E.right(expect.objectContaining({
          listName: list.name,
        })));
      });
    });
  });
});
