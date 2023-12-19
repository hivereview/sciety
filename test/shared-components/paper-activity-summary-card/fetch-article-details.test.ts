import { URL } from 'url';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { fetchArticleDetails } from '../../../src/shared-components/paper-activity-summary-card/fetch-article-details';
import * as DE from '../../../src/types/data-error';
import { toHtmlFragment } from '../../../src/types/html-fragment';
import { sanitise } from '../../../src/types/sanitised-html-fragment';
import { arbitraryArticleDetails } from '../../third-parties/external-queries.helper';
import { TestFramework, createTestFramework } from '../../framework';
import { arbitraryNumber, arbitraryUri } from '../../helpers';
import { arbitraryExpressionDoi } from '../../types/expression-doi.helper';

const titleText = 'Accuracy of predicting chemical body composition of growing pigs using dual-energy X-ray absorptiometry';

const getArticle = () => TE.right({
  ...arbitraryArticleDetails(),
  title: sanitise(toHtmlFragment(titleText)),
  server: 'biorxiv' as const,
  authors: O.some(['Kasper C', 'Schlegel P', 'Ruiz-Ascacibar I', 'Stoll P', 'Bee G']),
});

describe('fetch-article-details', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('latest version date', () => {
    it('returns the latest version date for an article', async () => {
      const expressionDoi = arbitraryExpressionDoi();
      const latestDate = new Date('2020-12-14');
      const articleDetails = await pipe(
        expressionDoi,
        fetchArticleDetails({
          ...framework.dependenciesForViews,
          findVersionsForArticleDoi: () => TO.some([{
            source: new URL(arbitraryUri()),
            publishedAt: latestDate,
            version: arbitraryNumber(1, 2),
          }]),
        }),
      )();

      expect(articleDetails).toStrictEqual(
        E.right(
          expect.objectContaining({
            latestVersionDate: O.some(latestDate),
          }),
        ),
      );
    });

    it('returns an O.none for the latest version date when it fails', async () => {
      const articleDetails = await pipe(
        arbitraryExpressionDoi(),
        fetchArticleDetails({
          ...framework.dependenciesForViews,
          findVersionsForArticleDoi: () => TO.none,
        }),
      )();

      expect(articleDetails).toStrictEqual(
        E.right(
          expect.objectContaining({
            latestVersionDate: O.none,
          }),
        ),
      );
    });
  });

  describe('getArticle', () => {
    it('returns on the left when getArticle fails', async () => {
      const articleDetails = await fetchArticleDetails(
        {
          ...framework.dependenciesForViews,
          fetchArticle: () => TE.left(DE.unavailable),
        },
      )(arbitraryExpressionDoi())();

      expect(articleDetails).toStrictEqual(E.left(DE.unavailable));
    });

    describe('title', () => {
      it('returns the title for an article', async () => {
        const expressionDoi = arbitraryExpressionDoi();
        const title = await pipe(
          expressionDoi,
          fetchArticleDetails(
            {
              ...framework.dependenciesForViews,
              fetchArticle: getArticle,
            },
          ),
          TE.map((article) => article.title),
        )();
        const expected = pipe(
          titleText,
          toHtmlFragment,
          sanitise,
          E.right,
        );

        expect(title).toStrictEqual(expected);
      });
    });

    describe('authors', () => {
      it('returns the authors for an article', async () => {
        const expressionDoi = arbitraryExpressionDoi();
        const authors = await pipe(
          expressionDoi,
          fetchArticleDetails(
            {
              ...framework.dependenciesForViews,
              fetchArticle: getArticle,
            },
          ),
          TE.map((article) => article.authors),
        )();
        const expected = pipe(
          O.some(['Kasper C', 'Schlegel P', 'Ruiz-Ascacibar I', 'Stoll P', 'Bee G']),
          E.right,
        );

        expect(authors).toStrictEqual(expected);
      });
    });
  });
});
