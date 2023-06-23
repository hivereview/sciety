import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { ArticleCardViewModel, renderArticleCard } from '../../../src/shared-components/article-card/render-article-card';
import { Doi } from '../../../src/types/doi';
import { toHtmlFragment } from '../../../src/types/html-fragment';
import { sanitise } from '../../../src/types/sanitised-html-fragment';
import { arbitraryArticleId } from '../../types/article-id.helper';

const generateArticleViewModel = ({
  articleId = arbitraryArticleId(),
  title = sanitise(toHtmlFragment('default title')),
  authors = O.some(['Smith J']),
  latestActivityDate = O.some(new Date()),
  latestVersionDate = O.some(new Date()),
  evaluationCount = 0,
  listMembershipCount = O.none,
}): ArticleCardViewModel => ({
  articleId,
  title,
  authors,
  latestActivityAt: latestActivityDate,
  latestVersionDate,
  evaluationCount,
  listMembershipCount,
  curationStatementsTeasers: [],
});

const getSpans = (articleViewModel: ArticleCardViewModel) => pipe(
  articleViewModel,
  renderArticleCard,
  JSDOM.fragment,
  (rendered) => rendered.querySelectorAll('span'),
);

describe('render-article-card', () => {
  it('links to the article page', () => {
    const articleViewModel = generateArticleViewModel({
      articleId: new Doi('10.1101/1234'),
      title: sanitise(toHtmlFragment('The article title')),
    });

    const rendered = JSDOM.fragment(renderArticleCard(articleViewModel));
    const link = rendered.querySelector('a');

    expect(link?.getAttribute('href')).toBe('/articles/activity/10.1101/1234');
  });

  describe('latest version', () => {
    const isLatestVersionSpan = (span: HTMLSpanElement) => span.textContent?.includes('Latest version');

    describe('when a latest version date is supplied', () => {
      it('displays the date', () => {
        const versionSpan = pipe(
          { latestVersionDate: O.some(new Date('1971-01-01')) },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.find(isLatestVersionSpan),
        );

        expect(versionSpan?.textContent).toBe('Latest version Jan 1, 1971');
      });
    });

    describe('when a latest version date is not supplied', () => {
      it('displays nothing', () => {
        const isLatestVersionPresent = pipe(
          { latestVersionDate: O.none },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.some(isLatestVersionSpan),
        );

        expect(isLatestVersionPresent).toBeFalsy();
      });
    });
  });

  describe('latest activity', () => {
    const isLatestActivitySpan = (span: HTMLSpanElement) => span.textContent?.includes('Latest activity');

    describe('when a latest activity date is supplied', () => {
      it('displays the date', () => {
        const latestActivitySpan = pipe(
          { latestActivityDate: O.some(new Date('1971-01-01')) },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.find(isLatestActivitySpan),
        );

        expect(latestActivitySpan?.textContent).toBe('Latest activity Jan 1, 1971');
      });
    });

    describe('when a latest activity date is not supplied', () => {
      it('displays nothing', () => {
        const isLatestActivityPresent = pipe(
          { latestActivityDate: O.none },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.some(isLatestActivitySpan),
        );

        expect(isLatestActivityPresent).toBeFalsy();
      });
    });
  });

  describe('authors', () => {
    const authorListSelector = '.card-authors';
    const authorListItemSelector = 'li.card-authors__author';

    describe('when the authors list is not empty', () => {
      const articleViewModel = generateArticleViewModel({
        authors: O.some(['Doe J', 'Foo A']),
      });

      it('the authors are in an ordered list', () => {
        const rendered = JSDOM.fragment(renderArticleCard(articleViewModel));
        const authors = rendered.querySelector(authorListSelector);

        expect(authors?.tagName).toBe('OL');
      });

      it('displays the authors as a list', () => {
        const rendered = JSDOM.fragment(renderArticleCard(articleViewModel));
        const authors = rendered.querySelectorAll(authorListItemSelector);
        const authorFullNames = Array.from(authors).map((element) => element.textContent);

        expect(authorFullNames).toStrictEqual([
          'Doe J',
          'Foo A',
        ]);
      });
    });

    describe('when the authors list is empty', () => {
      it('displays nothing', () => {
        const articleViewModel = generateArticleViewModel({ authors: O.some([]) });

        const rendered = JSDOM.fragment(renderArticleCard(articleViewModel));
        const authors = rendered.querySelector(authorListSelector);

        expect(authors).toBeNull();
      });
    });

    describe('when the authors list is missing', () => {
      it('displays nothing', () => {
        const articleViewModel = generateArticleViewModel({ authors: O.none });

        const rendered = JSDOM.fragment(renderArticleCard(articleViewModel));
        const authors = rendered.querySelector(authorListSelector);

        expect(authors).toBeNull();
      });
    });
  });

  describe('evaluations', () => {
    const isEvaluationSpan = (element: HTMLSpanElement) => element.textContent?.includes('evaluation');

    describe('when there are > 1 evaluations', () => {
      it('displays the evaluation count pluralised', () => {
        const evaluationsSpan = pipe(
          { evaluationCount: 42 },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.find(isEvaluationSpan),
        );

        expect(evaluationsSpan?.textContent).toBe('42 evaluations');
      });
    });

    describe('when there is 1 evaluation', () => {
      it('displays the evaluation count singular', () => {
        const evaluationsSpan = pipe(
          { evaluationCount: 1 },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.find(isEvaluationSpan),
        );

        expect(evaluationsSpan?.textContent).toBe('1 evaluation');
      });
    });

    describe('when there are 0 evaluations', () => {
      it('displays the evaluation count pluralised', () => {
        const evaluationsSpan = pipe(
          { evaluationCount: 0 },
          generateArticleViewModel,
          getSpans,
          Array.from,
          (array: Array<HTMLSpanElement>) => array.find(isEvaluationSpan),
        );

        expect(evaluationsSpan?.textContent).toBe('This article has no evaluations');
      });
    });
  });
});
