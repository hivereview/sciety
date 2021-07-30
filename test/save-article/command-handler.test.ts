import { commandHandler } from '../../src/save-article/command-handler';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('command-handler', () => {
  describe('article is saved', () => {
    describe('and a RemoveArticleFromUserList Command is issued', () => {
      it('creates an ArticleRemovedFromUserList Event', () => {
        const articleId = arbitraryDoi();
        const userId = arbitraryUserId();
        const saveState = 'saved';
        const removeArticleFromUserList = {
          type: 'RemoveArticleFromUserList' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(saveState, removeArticleFromUserList);

        expect(createdEvents).toStrictEqual([expect.objectContaining({
          type: 'UserUnsavedArticle',
          articleId,
          userId,
        })]);
      });
    });

    describe('and a SaveArticleToUserList Command is issued', () => {
      it('creates no events', () => {
        const articleId = arbitraryDoi();
        const userId = arbitraryUserId();
        const saveState = 'saved';
        const saveArticleToUserList = {
          type: 'SaveArticleToUserList' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(saveState, saveArticleToUserList);

        expect(createdEvents).toStrictEqual([]);
      });
    });
  });

  describe('article is not-saved', () => {
    describe('and a RemoveArticleFromUserList Command is issued', () => {
      it('creates no events', () => {
        const saveState = 'not-saved';
        const removeArticleFromUserList = {
          type: 'RemoveArticleFromUserList' as const,
          articleId: arbitraryDoi(),
          userId: arbitraryUserId(),
        };
        const createdEvents = commandHandler(saveState, removeArticleFromUserList);

        expect(createdEvents).toStrictEqual([]);
      });
    });

    describe('and a SaveArticleToUserList Command is issued', () => {
      it('creates a UserSavedArticle Event', () => {
        const articleId = arbitraryDoi();
        const userId = arbitraryUserId();
        const saveState = 'not-saved';
        const saveArticleToUserList = {
          type: 'SaveArticleToUserList' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(saveState, saveArticleToUserList);

        expect(createdEvents).toStrictEqual([expect.objectContaining({
          type: 'UserSavedArticle',
          articleId,
          userId,
        })]);
      });
    });
  });
});
