/* eslint-disable jest/prefer-lowercase-title */
import { DomainEvent, userSavedArticle, userUnsavedArticle } from '../../src/domain-events';
import { articleSaveState } from '../../src/save-article/article-save-state';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('article-save-state', () => {
  it('Bob has not saved any articles: false', () => {
    const events: ReadonlyArray<DomainEvent> = [];
    const articleInList = articleSaveState(arbitraryUserId(), arbitraryArticleId())(events);

    expect(articleInList).toBe('not-saved');
  });

  it('UserSavedArticle Bob, 1: saved', () => {
    const bob = arbitraryUserId();
    const articleId = arbitraryArticleId();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(bob, articleId),
    ];
    const articleInList = articleSaveState(bob, articleId)(events);

    expect(articleInList).toBe('saved');
  });

  it('UserSavedArticle Bob, 1; UserUnsavedArticle Bob, 1: not-saved', () => {
    const bob = arbitraryUserId();
    const articleId = arbitraryArticleId();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(bob, articleId),
      userUnsavedArticle(bob, articleId),
    ];
    const articleInList = articleSaveState(bob, articleId)(events);

    expect(articleInList).toBe('not-saved');
  });

  it('UserSavedArticle Bob, 1; UserUnsavedArticle Bob, 1; UserSavedArticle Bob, 1: saved', () => {
    const bob = arbitraryUserId();
    const articleId = arbitraryArticleId();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(bob, articleId),
      userUnsavedArticle(bob, articleId),
      userSavedArticle(bob, articleId),
    ];
    const articleInList = articleSaveState(bob, articleId)(events);

    expect(articleInList).toBe('saved');
  });

  it('Alice saved article 1 but Bob has not: not-saved', () => {
    const alice = arbitraryUserId();
    const bob = arbitraryUserId();
    const articleId = arbitraryArticleId();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(alice, articleId),
    ];
    const articleInList = articleSaveState(bob, articleId)(events);

    expect(articleInList).toBe('not-saved');
  });

  it('Bob has saved article 2: not-saved', () => {
    const bob = arbitraryUserId();
    const articleId1 = arbitraryArticleId();
    const articleId2 = arbitraryArticleId();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(bob, articleId2),
    ];
    const articleInList = articleSaveState(bob, articleId1)(events);

    expect(articleInList).toBe('not-saved');
  });
});
