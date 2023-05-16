import { DOMParser } from '@xmldom/xmldom';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import {
  getAbstract, getAuthors, getServer, getTitle,
} from './parse-crossref-article';
import { Logger } from '../../shared-ports';
import { ArticleAuthors } from '../../types/article-authors';
import { ArticleServer } from '../../types/article-server';
import * as DE from '../../types/data-error';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';
import { ExternalQueries } from '../external-queries';

type GetXml = (url: string, headers: Record<string, string>) => Promise<string>;

export const fetchCrossrefArticle = (
  getXml: GetXml,
  logger: Logger,
  crossrefApiBearerToken: O.Option<string>,
): ExternalQueries['fetchArticle'] => {
  const parser = new DOMParser({
    errorHandler: (_, msg) => {
      throw msg;
    },
  });

  return (doi) => async () => {
    // TODO:
    // pipe(
    //   fetch the xml,
    //   parse it,
    //   transform to our own Domain Model for an Article
    // )
    let response: string;
    try {
      const url = `https://api.crossref.org/works/${doi.value}/transform`;
      const headers: Record<string, string> = {
        Accept: 'application/vnd.crossref.unixref+xml',
        'User-Agent': 'Sciety (https://sciety.org; mailto:team@sciety.org)',
      };
      if (O.isSome(crossrefApiBearerToken)) {
        headers['Crossref-Plus-API-Token'] = `Bearer ${crossrefApiBearerToken.value}`;
      }
      response = await getXml(url, headers);
      if (response.length === 0) {
        throw new Error('Empty response from Crossref');
      }
    } catch (error: unknown) {
      const payload = {
        doi: doi.value,
        message: '',
      };
      let errorType: DE.DataError = DE.notFound;
      if (error instanceof Error) {
        payload.message = error.message;
        if (error.message === 'Empty response from Crossref') {
          errorType = DE.unavailable;
        }
      }
      logger('error', 'Failed to fetch article from Crossref', payload);
      return E.left(errorType);
    }

    let abstract: SanitisedHtmlFragment;
    let authors: ArticleAuthors;
    let server: O.Option<ArticleServer>;
    let title: SanitisedHtmlFragment;
    try {
      const doc = parser.parseFromString(response, 'text/xml');
      authors = getAuthors(doc);
      server = getServer(doc);

      if (O.isNone(authors)) {
        logger('error', 'Unable to find authors', { doi, response });
      }

      if (O.isNone(server)) {
        logger('warn', 'Unable to find server', { doi, response });
        return E.left(DE.notFound);
      }

      abstract = getAbstract(doc, doi, logger);
      title = getTitle(doc, doi, logger);
    } catch (error: unknown) {
      logger('error', 'Unable to parse document', { doi, response, error });

      // TODO: decide a product direction covering all scenarios:
      // - what happens with a 404?
      // - what happens with a 50x?
      // - what happens if the XML is corrupted?
      // - what happens if the title cannot be parsed (e.g. it's missing from the XML)?
      // - what happens if the abstract cannot be parsed (e.g. it has unforeseen tags)?
      // - ...
      return E.left(DE.unavailable);
    }
    return E.right({
      abstract,
      authors,
      doi,
      title,
      server: server.value,
    });
  };
};
