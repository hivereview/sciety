import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import {
  DomainEvent, isUserSavedArticleEvent, userSavedArticle, UserSavedArticleEvent,
} from '../../src/domain-events';
import { AddArticleToList, GetListsOwnedBy } from '../../src/shared-ports';
import * as LOID from '../../src/types/list-owner-id';
import { dummyLogger } from '../dummy-logger';
import { arbitraryList } from '../group-page/about/to-our-lists-view-model.test';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryListId } from '../types/list-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

type Ports = {
  addArticleToList: AddArticleToList,
  getListsOwnedBy: GetListsOwnedBy,
};

type AddArticleToGenericListFromUserSavedArticle = (ports: Ports) => (event: DomainEvent) => T.Task<undefined>;

const toCommand = (ports: Ports) => (event: UserSavedArticleEvent) => pipe(
  event.userId,
  LOID.fromUserId,
  ports.getListsOwnedBy,
  TE.map((lists) => ({ articleId: event.articleId, listId: lists[0].id })),
);

const addArticleToGenericListFromUserSavedArticle: AddArticleToGenericListFromUserSavedArticle = (
  ports,
) => (event) => pipe(
  event,
  E.fromPredicate(isUserSavedArticleEvent, () => 'not interesting'),
  TE.fromEither,
  TE.chainW(toCommand(ports)),
  TE.chain(ports.addArticleToList),
  TE.match(
    () => undefined,
    () => undefined,
  ),
);

describe('add-article-to-generic-list-from-user-saved-article', () => {
  const defaultPorts = {
    addArticleToList: () => TE.right(undefined),
    logger: dummyLogger,
  };

  let ports: Ports;

  describe('when a UserSavedArticle event is received', () => {
    describe('and the user has a generic list', () => {
      const userId = arbitraryUserId();
      const articleId = arbitraryArticleId();
      const listId = arbitraryListId();

      const event = userSavedArticle(userId, articleId);

      beforeEach(async () => {
        ports = {
          addArticleToList: jest.fn(defaultPorts.addArticleToList),
          getListsOwnedBy: () => TE.right([{
            ...arbitraryList(),
            id: listId,
          }]),
        };
        await addArticleToGenericListFromUserSavedArticle(ports)(event)();
      });

      it('calls the AddArticleToList command', () => {
        expect(ports.addArticleToList).toHaveBeenCalledWith(expect.anything());
      });

      it('calls the command with the generic list id owned by that user', () => {
        expect(ports.addArticleToList).toHaveBeenCalledWith(
          expect.objectContaining({ listId }),
        );
      });

      it('calls the command with the article id in the UserSavedArticle event', () => {
        expect(ports.addArticleToList).toHaveBeenCalledWith(expect.objectContaining({
          articleId,
        }));
      });
    });

    describe('and the user does not have a generic list', () => {
      it.todo('logs an error');
    });
  });

  describe('when any other event is received', () => {
    it.todo('does not call the AddArticleToList command');
  });
});
