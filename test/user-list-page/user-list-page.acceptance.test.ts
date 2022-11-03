import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { userSavedArticle } from '../../src/domain-events';
import * as DE from '../../src/types/data-error';
import { Doi } from '../../src/types/doi';
import { Page } from '../../src/types/page';
import { RenderPageError } from '../../src/types/render-page-error';
import { userListPage } from '../../src/user-list-page';
import { informationUnavailable, noSavedArticles } from '../../src/user-list-page/saved-articles/static-messages';
import {
  arbitrarySanitisedHtmlFragment, arbitraryString, arbitraryUri, arbitraryWord,
} from '../helpers';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryUserId } from '../types/user-id.helper';

const contentOf = (page: TE.TaskEither<RenderPageError, Page>) => pipe(
  page,
  TE.match(
    (errorPage) => errorPage.message,
    (p) => p.content,
  ),
);

const arbitraryUserDetails = {
  avatarUrl: arbitraryUri(),
  displayName: arbitraryString(),
  handle: arbitraryWord(),
  userId: arbitraryUserId(),
};

const defaultPorts = {
  getUserDetails: () => TE.right(arbitraryUserDetails),
  getAllEvents: T.of([]),
  fetchArticle: () => TE.right({
    doi: arbitraryDoi(),
    server: 'biorxiv' as const,
    title: arbitrarySanitisedHtmlFragment(),
    authors: O.none,
  }),
  getUserId: () => TE.right(arbitraryUserId()),
  findReviewsForArticleDoi: () => TE.right([]),
  findVersionsForArticleDoi: () => TO.none,
};

describe('user-list-page', () => {
  it('uses the user handle in the page title', async () => {
    const handle = arbitraryWord();
    const ports = {
      getUserDetails: () => TE.right({
        avatarUrl: arbitraryUri(),
        displayName: arbitraryString(),
        handle,
        userId: arbitraryUserId(),
      }),
      getAllEvents: T.of([]),
      fetchArticle: shouldNotBeCalled,
      findReviewsForArticleDoi: shouldNotBeCalled,
      findVersionsForArticleDoi: shouldNotBeCalled,
      getUserId: () => TE.right(arbitraryUserId()),
    };
    const params = { handle, user: O.none, page: 1 };
    const page = await pipe(
      params,
      userListPage(ports),
    )();

    expect(page).toStrictEqual(
      E.right(expect.objectContaining({
        title: expect.stringContaining(handle),
      })),
    );
  });

  describe('when the user has saved articles', () => {
    it('shows the articles as a list of cards', async () => {
      const userId = arbitraryUserId();
      const ports = {
        getUserDetails: () => TE.right({
          avatarUrl: arbitraryUri(),
          displayName: arbitraryString(),
          handle: arbitraryWord(),
          userId,
        }),
        getAllEvents: T.of([
          userSavedArticle(userId, arbitraryArticleId()),
          userSavedArticle(userId, arbitraryArticleId()),
        ]),
        fetchArticle: () => TE.right({
          doi: arbitraryDoi(),
          server: 'biorxiv' as const,
          title: arbitrarySanitisedHtmlFragment(),
          authors: O.none,
        }),
        findReviewsForArticleDoi: () => TE.right([]),
        findVersionsForArticleDoi: () => TO.none,
        getUserId: () => TE.right(userId),
      };
      const params = { handle: arbitraryWord(), user: O.none, page: 1 };

      const page = await pipe(
        params,
        userListPage(ports),
        contentOf,
        T.map(JSDOM.fragment),
      )();
      const articleCards = page.querySelectorAll('.article-card');

      expect(articleCards).toHaveLength(2);
    });

    describe('article details unavailable for all articles', () => {
      it('displays the error message', async () => {
        const userId = arbitraryUserId();
        const ports = {
          ...defaultPorts,
          getAllEvents: T.of([
            userSavedArticle(userId, arbitraryArticleId()),
            userSavedArticle(userId, arbitraryArticleId()),
          ]),
          fetchArticle: () => TE.left('unavailable'),
          getUserId: () => TE.right(userId),
        };
        const params = { handle: arbitraryWord(), user: O.none, page: 1 };

        const pageContent = await pipe(
          params,
          userListPage(ports),
          contentOf,
          T.map(JSDOM.fragment),
        )();

        const message = pageContent.querySelector('.static-message')?.outerHTML;

        expect(message).toContain(informationUnavailable);
      });
    });

    describe('article details unavailable for some articles', () => {
      it('displays the remaining available articles', async () => {
        const failingArticleId = arbitraryDoi();
        const userId = arbitraryUserId();
        const ports = {
          getUserDetails: () => TE.right({
            avatarUrl: arbitraryUri(),
            displayName: arbitraryString(),
            handle: arbitraryWord(),
            userId,
          }),
          getAllEvents: T.of([
            userSavedArticle(userId, arbitraryArticleId()),
            userSavedArticle(userId, failingArticleId),
            userSavedArticle(userId, arbitraryArticleId()),
          ]),
          fetchArticle: (articleId: Doi) => (
            articleId.value === failingArticleId.value
              ? TE.left(DE.notFound)
              : TE.right({
                doi: articleId,
                server: 'biorxiv' as const,
                title: arbitrarySanitisedHtmlFragment(),
                authors: O.none,
              })
          ),
          findReviewsForArticleDoi: () => TE.right([]),
          findVersionsForArticleDoi: () => TO.none,
          getUserId: () => TE.right(userId),
        };
        const params = { handle: arbitraryWord(), user: O.none, page: 1 };

        const page = await pipe(
          params,
          userListPage(ports),
          contentOf,
          T.map(JSDOM.fragment),
        )();
        const articleCards = page.querySelectorAll('.article-card');

        expect(articleCards).toHaveLength(2);
      });
    });

    describe('when the logged in user is the list owner', () => {
      it('displays the delete buttons on the article cards', async () => {
        const owningUserId = arbitraryUserId();
        const ports = {
          getUserDetails: () => TE.right({
            avatarUrl: arbitraryUri(),
            displayName: arbitraryString(),
            handle: arbitraryWord(),
            userId: owningUserId,
          }),
          getAllEvents: T.of([
            userSavedArticle(owningUserId, arbitraryArticleId()),
            userSavedArticle(owningUserId, arbitraryArticleId()),
          ]),
          fetchArticle: () => TE.right({
            doi: arbitraryDoi(),
            server: 'biorxiv' as const,
            title: arbitrarySanitisedHtmlFragment(),
            authors: O.none,
          }),
          findReviewsForArticleDoi: () => TE.right([]),
          findVersionsForArticleDoi: () => TO.none,
          getUserId: () => TE.right(owningUserId),
        };
        const params = { handle: arbitraryWord(), user: O.some({ id: owningUserId }), page: 1 };

        const page = await pipe(
          params,
          userListPage(ports),
          contentOf,
          T.map(JSDOM.fragment),
        )();
        const deleteButtons = page.querySelectorAll('.saved-articles-control');

        expect(deleteButtons).toHaveLength(2);
      });
    });

    describe('when the logged in user is not the list owner', () => {
      it('does not display the delete buttons on the article cards', async () => {
        const owningUserId = arbitraryUserId();
        const loggedInUserId = arbitraryUserId();
        const ports = {
          getUserDetails: () => TE.right(arbitraryUserDetails),
          getAllEvents: T.of([
            userSavedArticle(owningUserId, arbitraryArticleId()),
            userSavedArticle(owningUserId, arbitraryArticleId()),
          ]),
          fetchArticle: () => TE.right({
            doi: arbitraryDoi(),
            server: 'biorxiv' as const,
            title: arbitrarySanitisedHtmlFragment(),
            authors: O.none,
          }),
          findReviewsForArticleDoi: () => TE.right([]),
          findVersionsForArticleDoi: () => TO.none,
          getUserId: () => TE.right(owningUserId),
        };
        const params = { handle: arbitraryWord(), user: O.some({ id: loggedInUserId }), page: 1 };

        const page = await pipe(
          params,
          userListPage(ports),
          contentOf,
          T.map(JSDOM.fragment),
        )();
        const deleteButtons = page.querySelectorAll('.saved-articles-control');

        expect(deleteButtons).toHaveLength(0);
      });
    });
  });

  describe('when the user has no saved articles', () => {
    let page: DocumentFragment;

    beforeAll(async () => {
      const params = { handle: arbitraryWord(), user: O.none, page: 1 };

      page = await pipe(
        params,
        userListPage(defaultPorts),
        contentOf,
        T.map(JSDOM.fragment),
      )();
    });

    it('shows no list of article cards', () => {
      const articleCards = page.querySelectorAll('.article-card');

      expect(articleCards).toHaveLength(0);
    });

    it('shows a message saying that the user has no saved articles', () => {
      const message = page.querySelector('.static-message')?.outerHTML;

      expect(message).toContain(noSavedArticles);
    });
  });
});
