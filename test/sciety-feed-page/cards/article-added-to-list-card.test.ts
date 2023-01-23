import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { articleAddedToList } from '../../../src/domain-events';
import { articleAddedToListCard, Ports } from '../../../src/sciety-feed-page/cards/article-added-to-list-card';
import { dummyLogger } from '../../dummy-logger';
import { arbitraryString, arbitraryUri } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryList } from '../../types/list-helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryUserId } from '../../types/user-id.helper';
import { GetList } from '../../../src/shared-ports';

describe('article-added-to-list-card', () => {
  describe('when a group owns the list', () => {
    it.todo('write tests');
  });

  describe('when a user owns the list', () => {
    const date = new Date('2021-09-15');
    const listId = arbitraryListId();
    const event = articleAddedToList(arbitraryArticleId(), listId, date);
    const getAllEvents = T.of([]);
    const getList: GetList = () => O.some({
      ...arbitraryList(),
      id: listId,
    });

    describe('when user details are available', () => {
      const avatarUrl = arbitraryUri();
      const handle = 'handle';
      const ports: Ports = {
        getAllEvents,
        getList,
        getUser: () => O.some({
          handle,
          avatarUrl,
          id: arbitraryUserId(),
          displayName: arbitraryString(),
        }),
        getGroup: () => O.some(arbitraryGroup()),
        logger: dummyLogger,
      };

      const viewModel = pipe(
        event,
        articleAddedToListCard(ports),
        O.getOrElseW(shouldNotBeCalled),
      );

      it('includes the user\'s handle in the title text', async () => {
        expect(viewModel.titleText).toContain(handle);
      });

      it('includes the user\'s avatar', async () => {
        expect(viewModel.avatarUrl).toStrictEqual(avatarUrl);
      });

      it('includes the event date', async () => {
        expect(viewModel.date).toStrictEqual(date);
      });

      it('includes the link to the list page', async () => {
        expect(viewModel.linkUrl).toBe(`/lists/${listId}`);
      });
    });

    describe('when user details are not found', () => {
      const ports: Ports = {
        getAllEvents,
        getList,
        getUser: () => O.none,
        getGroup: () => O.some(arbitraryGroup()),
        logger: dummyLogger,
      };

      const viewModel = pipe(
        event,
        articleAddedToListCard(ports),
        O.getOrElseW(shouldNotBeCalled),
      );

      it('replaces handle with "a user"', async () => {
        expect(viewModel.titleText).toMatch(/^A user/);
      });

      it('replaces avatar with a default image', async () => {
        expect(viewModel.avatarUrl).toBe('/static/images/sciety-logo.jpg');
      });

      it('includes the event date', async () => {
        expect(viewModel.date).toStrictEqual(date);
      });

      it('includes the link to the generic list page', async () => {
        expect(viewModel.linkUrl).toBe(`/lists/${listId}`);
      });
    });
  });
});
