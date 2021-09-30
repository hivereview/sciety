import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { evaluatedArticlesList, Ports } from '../../../src/group-evaluations-page/evaluated-articles-list';
import * as DE from '../../../src/types/data-error';
import { Doi } from '../../../src/types/doi';
import { HtmlFragment } from '../../../src/types/html-fragment';
import { arbitraryDate, arbitraryNumber, arbitrarySanitisedHtmlFragment } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleServer } from '../../types/article-server.helper';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroup } from '../../types/group.helper';

const generateArticles = (count: number) => (
  [...Array(count).keys()].map(() => ({
    doi: arbitraryDoi(),
    evaluationCount: arbitraryNumber(1, 5),
    latestActivityDate: arbitraryDate(),
  }))
);

const findVersionsForArticleDoi: Ports['findVersionsForArticleDoi'] = () => TO.some([{ occurredAt: arbitraryDate() }]);

const cardCount = (html: HtmlFragment) => Array.from(JSDOM.fragment(html).querySelectorAll('.article-card')).length;

describe('evaluated-articles-list', () => {
  describe('when article details for the page can be fetched', () => {
    const fetchArticle = () => TE.right({
      title: arbitrarySanitisedHtmlFragment(),
      server: arbitraryArticleServer(),
      authors: [],
    });

    it('returns article cards for each article', async () => {
      const articles = generateArticles(2);
      const html = await pipe(
        evaluatedArticlesList({
          fetchArticle,
          findVersionsForArticleDoi,
        })(articles, arbitraryGroup(), 1, 20),
        TE.getOrElse(shouldNotBeCalled),
      )();

      expect(cardCount(html)).toBe(2);
    });

    it.todo('shows "page x of y"');
  });

  describe('when there are no evaluated articles', () => {
    it('displays a static message', async () => {
      const html = await pipe(
        evaluatedArticlesList({
          fetchArticle: shouldNotBeCalled,
          findVersionsForArticleDoi: shouldNotBeCalled,
        })([], arbitraryGroup(), 1, 20),
        TE.getOrElse(shouldNotBeCalled),
      )();

      expect(html).toContain('hasn’t evaluated any articles');
    });

    it.todo('doesn\'t show "page x of y"');
  });

  describe('when there is more than one page', () => {
    const fetchArticle = () => TE.right({
      title: arbitrarySanitisedHtmlFragment(),
      server: arbitraryArticleServer(),
      authors: [],
    });

    it('links to the next page', async () => {
      const group = arbitraryGroup();
      const articleCount = 20;
      const articles = generateArticles(articleCount);
      const html = await pipe(
        evaluatedArticlesList({
          fetchArticle,
          findVersionsForArticleDoi,
        })(articles, group, 1, arbitraryNumber(2, articleCount - 1)),
        TE.getOrElse(shouldNotBeCalled),
      )();
      const link = `/groups/${group.slug}/evaluated-articles?page=2`;

      expect(html).toContain(link);
    });

    it.todo('shows "page x of y"');
  });

  describe('when some of the article details can\'t be retrieved', () => {
    it('returns the successful article cards', async () => {
      const articles = generateArticles(4);
      const fetchArticle = (doi: Doi) => {
        if (doi.value === articles[0].doi.value || doi.value === articles[1].doi.value) {
          return TE.left(DE.unavailable);
        }
        return TE.right({
          title: arbitrarySanitisedHtmlFragment(),
          server: arbitraryArticleServer(),
          authors: [],
        });
      };
      const html = await pipe(
        evaluatedArticlesList({
          fetchArticle,
          findVersionsForArticleDoi,
        })(articles, arbitraryGroup(), 1, 20),
        TE.getOrElse(shouldNotBeCalled),
      )();

      expect(cardCount(html)).toBe(2);
    });

    it.todo('shows "page x of y"');
  });

  describe('when none of the article details can be retrieved', () => {
    it('returns "this information can\'t be found" message', async () => {
      const result = await pipe(
        evaluatedArticlesList({
          fetchArticle: () => TE.left(DE.unavailable),
          findVersionsForArticleDoi,
        })(
          generateArticles(1),
          arbitraryGroup(),
          1,
          1,
        ),
        TE.getOrElse(shouldNotBeCalled),
      )();

      expect(result).toContain('This information can not be found');
    });

    it.todo('doesn\'t show "page x of y"');
  });

  describe('when the requested page is out of bounds', () => {
    it('returns not found', async () => {
      const pageNumber = 2;
      const pageSize = 1;
      const result = await evaluatedArticlesList({
        fetchArticle: shouldNotBeCalled,
        findVersionsForArticleDoi,
      })(
        generateArticles(1),
        arbitraryGroup(),
        pageNumber,
        pageSize,
      )();

      expect(result).toStrictEqual(E.left(DE.notFound));
    });
  });
});
