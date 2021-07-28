/* eslint-disable jest/lowercase-name */
import { DomainEvent, userSavedArticle } from '../../src/types/domain-events';
import { stateManager } from '../../src/user-list/state-manager';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('state-manager', () => {
  it('Bob has not saved any articles: false', () => {
    const events: ReadonlyArray<DomainEvent> = [];
    const articleInList = stateManager(events);

    expect(articleInList).toBe(false);
  });

  it.skip('UserSavedArticle Bob, 1: true', () => {
    const bob = arbitraryUserId();
    const article1 = arbitraryDoi();
    const events: ReadonlyArray<DomainEvent> = [
      userSavedArticle(bob, article1),
    ];
    const articleInList = stateManager(events);

    expect(articleInList).toBe(true);
  });

  it.todo('UserSavedArticle Bob, 1; ArticleRemovedFromUserList Bob, 1: false');

  it.todo('UserSavedArticle Bob, 1; ArticleRemovedFromUserList Bob, 1; UserSavedArticle Bob, 1: true');

  it.todo('Alice saved article 1 but Bob has not: false');

  it.todo('Bob has saved article 2: false');
});
