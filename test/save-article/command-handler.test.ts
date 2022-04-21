import { commandHandler } from '../../src/save-article/command-handler';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('command-handler', () => {
  describe('article is saved', () => {
    describe('and a UnsaveArticle Command is issued', () => {
      it('creates an UserUnsavedArticle Event', () => {
        const articleId = arbitraryArticleId();
        const userId = arbitraryUserId();
        const saveState = 'saved';
        const unsaveArticle = {
          type: 'UnsaveArticle' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(unsaveArticle)(saveState);

        expect(createdEvents).toStrictEqual([expect.objectContaining({
          type: 'UserUnsavedArticle',
          articleId,
          userId,
        })]);
      });
    });

    describe('and a SaveArticle Command is issued', () => {
      it('creates no events', () => {
        const articleId = arbitraryArticleId();
        const userId = arbitraryUserId();
        const saveState = 'saved';
        const saveArticle = {
          type: 'SaveArticle' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(saveArticle)(saveState);

        expect(createdEvents).toStrictEqual([]);
      });
    });
  });

  describe('article is not-saved', () => {
    describe('and a UnsaveArticle Command is issued', () => {
      it('creates no events', () => {
        const saveState = 'not-saved';
        const unsaveArticle = {
          type: 'UnsaveArticle' as const,
          articleId: arbitraryArticleId(),
          userId: arbitraryUserId(),
        };
        const createdEvents = commandHandler(unsaveArticle)(saveState);

        expect(createdEvents).toStrictEqual([]);
      });
    });

    describe('and a SaveArticle Command is issued', () => {
      it('creates a UserSavedArticle Event', () => {
        const articleId = arbitraryArticleId();
        const userId = arbitraryUserId();
        const saveState = 'not-saved';
        const saveArticle = {
          type: 'SaveArticle' as const,
          articleId,
          userId,
        };
        const createdEvents = commandHandler(saveArticle)(saveState);

        expect(createdEvents).toStrictEqual([expect.objectContaining({
          type: 'UserSavedArticle',
          articleId,
          userId,
        })]);
      });
    });
  });
});
