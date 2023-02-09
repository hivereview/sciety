import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import * as DE from '../../../../src/types/data-error';
import { articlePage, Ports } from '../../../../src/html-pages/article-page';
import { SanitisedHtmlFragment } from '../../../../src/types/sanitised-html-fragment';
import { arbitrarySanitisedHtmlFragment } from '../../../helpers';
import { arbitraryDoi } from '../../../types/doi.helper';

describe('correct-language-semantics', () => {
  describe('in the article page', () => {
    const defaultAdapters: Ports = {
      fetchArticle: () => TE.left(DE.unavailable),
      fetchReview: () => TE.left(DE.unavailable),
      findVersionsForArticleDoi: () => TO.none,
      getAllEvents: T.of([]),
      isArticleOnTheListOwnedBy: () => () => O.none,
      getGroup: () => O.none,
    };

    describe('the article title', () => {
      const createGetArticleDetails = (title: string) => () => (TE.right({
        doi: arbitraryDoi(),
        title: title as SanitisedHtmlFragment,
        abstract: arbitrarySanitisedHtmlFragment(),
        server: 'biorxiv' as const,
        authors: O.none,
      }));

      it.each([
        ['en', 'Arbitrary title in English'],
        ['es', 'Título arbitrario en español'],
        ['pt', 'Título arbitrário em português'],
      ])('is correctly inferred as %s', async (code, title) => {
        const adapters = {
          ...defaultAdapters,
          fetchArticle: createGetArticleDetails(title),
        };
        const renderPage = articlePage(adapters);
        const rendered = await renderPage({
          doi: arbitraryDoi(),
          user: O.none,
        })();

        expect(rendered).toStrictEqual(E.right(expect.objectContaining({
          content: expect.stringContaining(`<h1 lang="${code}">${title}</h1>`),
        })));
      });
    });

    describe('the article abstract', () => {
      const createGetArticleDetails = (abstract: string) => () => (TE.right({
        doi: arbitraryDoi(),
        title: arbitrarySanitisedHtmlFragment(),
        abstract: abstract as SanitisedHtmlFragment,
        server: 'biorxiv' as const,
        authors: O.none,
      }));

      it.each([
        ['en', 'This text represents the abstract of this article in English.'],
        ['es', 'Este texto representa el resumen de este artículo en español.'],
        ['pt', 'Este texto representa o resumo deste artigo em português.'],
      ])('is correctly inferred as %s', async (code, articleAbstract) => {
        const adapters = {
          ...defaultAdapters,
          fetchArticle: createGetArticleDetails(articleAbstract),
        };
        const renderPage = articlePage(adapters);
        const rendered = await renderPage({
          doi: arbitraryDoi(),
          user: O.none,
        })();

        expect(rendered).toStrictEqual(E.right(expect.objectContaining({
          content: expect.stringContaining(`<div lang="${code}">${articleAbstract}</div>`),
        })));
      });
    });
  });

  describe.skip('in the article card', () => {
    it.todo('to be done later');
  });
});
