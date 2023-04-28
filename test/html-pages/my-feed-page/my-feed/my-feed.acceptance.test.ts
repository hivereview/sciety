import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { JSDOM } from 'jsdom';
import { evaluationRecorded, userFollowedEditorialCommunity } from '../../../../src/domain-events';
import { myFeed, Ports } from '../../../../src/html-pages/my-feed-page/my-feed';
import {
  feedTitle,
  followSomething,
  noEvaluationsYet,
  troubleFetchingTryAgain,
} from '../../../../src/html-pages/my-feed-page/my-feed/static-content';
import { FindVersionsForArticleDoi } from '../../../../src/shared-ports';
import * as DE from '../../../../src/types/data-error';
import { Doi, eqDoi } from '../../../../src/types/doi';
import { toHtmlFragment } from '../../../../src/types/html-fragment';
import { sanitise } from '../../../../src/types/sanitised-html-fragment';
import { arbitraryDate, arbitraryNumber, arbitraryUri } from '../../../helpers';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { arbitraryArticleId } from '../../../types/article-id.helper';
import { arbitraryDoi } from '../../../types/doi.helper';
import { arbitraryGroupId } from '../../../types/group-id.helper';
import { arbitraryReviewId } from '../../../types/review-id.helper';
import { arbitraryUserId } from '../../../types/user-id.helper';

const defaultAdapters: Ports = {
  fetchArticle: shouldNotBeCalled,
  findVersionsForArticleDoi: shouldNotBeCalled,
  getAllEvents: T.of([]),
  getGroupsFollowedBy: () => [],
};

describe('my-feed acceptance', () => {
  it('displays the feed title', async () => {
    const html = await myFeed(defaultAdapters)(arbitraryUserId(), 20, 1)();

    expect(html).toContain(feedTitle);
  });

  describe('there is a logged in user', () => {
    const userId = arbitraryUserId();

    describe('following groups that have no evaluations', () => {
      const groupId = arbitraryGroupId();

      it('displays the calls to action to follow other groups or return later', async () => {
        const adapters = {
          ...defaultAdapters,
          getAllEvents: T.of([userFollowedEditorialCommunity(userId, groupId)]),
          getGroupsFollowedBy: () => [groupId],
        };

        const html = await myFeed(adapters)(userId, 20, 1)();

        expect(html).toContain(noEvaluationsYet);
      });
    });

    // Your feed is empty! Start following some groups to see their most recent evaluations right here.
    describe('not following any groups', () => {
      it('displays call to action to follow groups', async () => {
        const adapters = {
          ...defaultAdapters,
          getAllEvents: T.of([]),
        };
        const html = await myFeed(adapters)(userId, 20, 1)();

        expect(html).toContain(followSomething);
      });
    });

    describe('following groups with evaluations', () => {
      const arbitraryVersions = () => TO.some([
        {
          source: new URL(arbitraryUri()),
          version: arbitraryNumber(1, 5),
          publishedAt: new Date(),
        },
      ]) as ReturnType<FindVersionsForArticleDoi>;

      it('displays content in the form of article cards', async () => {
        const groupId = arbitraryGroupId();
        const adapters = {
          ...defaultAdapters,
          fetchArticle: () => TE.right({
            title: sanitise(toHtmlFragment('My article title')),
            authors: O.none,
            server: 'biorxiv' as const,
          }),
          findVersionsForArticleDoi: arbitraryVersions,
          getAllEvents: T.of([
            userFollowedEditorialCommunity(userId, groupId),
            evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
          ]),
          getGroupsFollowedBy: () => [groupId],
        };
        const html = await myFeed(adapters)(userId, 20, 1)();

        expect(html).toContain('class="article-card"');
      });

      it('renders at most a page of cards at a time', async () => {
        const groupId = arbitraryGroupId();
        const adapters = {
          ...defaultAdapters,
          fetchArticle: () => TE.right({
            title: sanitise(toHtmlFragment('My article title')),
            authors: O.none,
            server: 'biorxiv' as const,
          }),
          findVersionsForArticleDoi: arbitraryVersions,
          getAllEvents: T.of([
            userFollowedEditorialCommunity(userId, groupId),
            evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
            evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
            evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
          ]),
          getGroupsFollowedBy: () => [groupId],
        };
        const pageSize = 2;
        const renderedComponent = await myFeed(adapters)(userId, pageSize, 1)();
        const html = JSDOM.fragment(renderedComponent);
        const itemCount = Array.from(html.querySelectorAll('.article-card')).length;

        expect(itemCount).toStrictEqual(pageSize);
      });

      it.todo('displays the articles in order of latest activity in descending order');

      it.todo('latest activity is based off of activity by any group');

      it.todo('each article is only displayed once');

      it('displayed articles have to have been evaluated by a followed group', async () => {
        const groupId = arbitraryGroupId();
        const adapters = {
          ...defaultAdapters,
          fetchArticle: () => TE.right({
            title: sanitise(toHtmlFragment('My article title')),
            authors: O.none,
            server: 'biorxiv' as const,
          }),
          findVersionsForArticleDoi: arbitraryVersions,
          getAllEvents: T.of([
            userFollowedEditorialCommunity(userId, groupId),
            evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
          ]),
          getGroupsFollowedBy: () => [groupId],
        };
        const html = await myFeed(adapters)(userId, 20, 1)();

        expect(html).toContain('My article title');
      });

      describe('when details of an article cannot be fetched', () => {
        it('only displays the successfully fetched articles', async () => {
          const groupId = arbitraryGroupId();
          const failingDoi = arbitraryDoi();
          const adapters = {
            ...defaultAdapters,
            fetchArticle: (doi: Doi) => (
              eqDoi.equals(doi, failingDoi)
                ? TE.left(DE.unavailable)
                : TE.right({
                  title: sanitise(toHtmlFragment('My article title')),
                  authors: O.none,
                  server: 'biorxiv' as const,
                })),
            findVersionsForArticleDoi: arbitraryVersions,
            getAllEvents: T.of([
              userFollowedEditorialCommunity(userId, groupId),
              evaluationRecorded(groupId, failingDoi, arbitraryReviewId(), [], arbitraryDate()),
              evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
            ]),
            getGroupsFollowedBy: () => [groupId],
          };

          const html = await myFeed(adapters)(userId, 20, 1)();
          const fragment = JSDOM.fragment(html);
          const cards = Array.from(fragment.querySelectorAll('.article-card'));

          expect(cards).toHaveLength(1);
        });
      });

      describe('when details of all articles cannot be fetched', () => {
        it('display only an error message', async () => {
          const groupId = arbitraryGroupId();
          const adapters = {
            ...defaultAdapters,
            fetchArticle: () => TE.left(DE.unavailable),
            getAllEvents: T.of([
              userFollowedEditorialCommunity(userId, groupId),
              evaluationRecorded(groupId, arbitraryArticleId(), arbitraryReviewId(), [], arbitraryDate()),
            ]),
            getGroupsFollowedBy: () => [groupId],
          };
          const html = await myFeed(adapters)(userId, 20, 1)();

          expect(html).toContain(troubleFetchingTryAgain);
        });
      });
    });
  });
});
