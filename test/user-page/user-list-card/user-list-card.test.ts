import { JSDOM } from 'jsdom';
import { userListCard } from '../../../src/user-page/user-list-card';
import { arbitraryWord } from '../../helpers';

describe('user-list-card', () => {
  it('displays the title of the list', async () => {
    const rendered = JSDOM.fragment(await userListCard(arbitraryWord())());

    expect(rendered?.textContent).toContain('Saved articles');
  });

  it('the title holds a link', async () => {
    const rendered = JSDOM.fragment(await userListCard(arbitraryWord())());

    expect(rendered.querySelector('h3 a')).not.toBeNull();
  });

  it.todo('displays the list owner\'s handle in the description');

  describe('when list contains articles', () => {
    it.todo('displays when the list was last updated');

    it.todo('displays the number of articles in the list');
  });

  describe('when list is empty', () => {
    it.todo('does not display last updated date');

    it.todo('displays an article count of 0');
  });
});
