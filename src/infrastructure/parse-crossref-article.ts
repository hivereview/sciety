import * as O from 'fp-ts/Option';
import { constant, flow, pipe } from 'fp-ts/function';
import { XMLSerializer } from 'xmldom';
import { Logger } from './logger';
import { ArticleServer } from '../types/article-server';
import { Doi } from '../types/doi';
import { toHtmlFragment } from '../types/html-fragment';
import { sanitise, SanitisedHtmlFragment } from '../types/sanitised-html-fragment';

const getElement = (ancestor: Document | Element, qualifiedName: string): Element | null => (
  ancestor.getElementsByTagName(qualifiedName).item(0)
);

export const getAbstract = (doc: Document, doi: Doi, logger: Logger): SanitisedHtmlFragment => {
  const abstractElement = getElement(doc, 'abstract');

  if (typeof abstractElement?.textContent !== 'string') {
    logger('warn', 'Did not find abstract', { doi });

    return pipe(
      `No abstract for ${doi.value} available`,
      toHtmlFragment,
      sanitise,
    );
  }

  logger('debug', 'Found abstract', { doi, abstract: abstractElement.textContent });

  const titleElement = getElement(abstractElement, 'title');
  if (titleElement) {
    abstractElement.removeChild(titleElement);
  }

  const titles = abstractElement.getElementsByTagName('title');
  for (let i = 0; i < titles.length; i += 1) {
    const title = titles.item(i);
    if (title) {
      if (title.textContent === 'Graphical abstract') {
        abstractElement.removeChild(title);
      }
    }
  }

  const abstract = new XMLSerializer().serializeToString(abstractElement);

  const transformXmlToHtml = (xml: string): string => (
    xml
      .replace(/<abstract[^>]*>/, '')
      .replace(/<\/abstract>/, '')
      .replace(/<italic[^>]*>/g, '<i>')
      .replace(/<\/italic>/g, '</i>')
      .replace(/<list[^>]* list-type=['"]bullet['"][^>]*/g, '<ul')
      .replace(/<\/list>/g, '</ul>')
      .replace(/<list-item[^>]*/g, '<li')
      .replace(/<\/list-item>/g, '</li>')
      .replace(/<sec[^>]*/g, '<section')
      .replace(/<\/sec>/g, '</section>')
      .replace(/<title[^>]*/g, '<h3')
      .replace(/<\/title>/g, '</h3>')
  );

  const stripEmptySections = (html: string): string => (
    html.replace(/<section>\s*<\/section>/g, '')
  );

  return pipe(
    abstract,
    transformXmlToHtml,
    toHtmlFragment,
    sanitise,
    stripEmptySections,
    toHtmlFragment,
    sanitise,
  );
};

export const getTitle = (doc: Document, doi: Doi, logger: Logger): SanitisedHtmlFragment => {
  const titlesElement = getElement(doc, 'titles');
  const titleElement = titlesElement?.getElementsByTagName('title')[0];
  let title = 'Unknown title';
  // TODO: the decision as to what to display on error should live with the rendering component
  if (titleElement) {
    title = new XMLSerializer()
      .serializeToString(titleElement)
      .replace(/^<title(?:.?)>([\s\S]*)<\/title>$/, '$1');
  } else {
    logger('warn', 'Did not find title', { doi });
  }

  return pipe(
    title,
    toHtmlFragment,
    sanitise,
  );
};

export const getPublicationDate = (doc: Document): Date => {
  const postedDateElement = getElement(doc, 'posted_date');

  const postedDateYear = postedDateElement?.getElementsByTagName('year')[0];
  const year = postedDateYear?.textContent ?? '1970';

  const postedDateMonth = postedDateElement?.getElementsByTagName('month')[0];
  const month = postedDateMonth?.textContent ?? '01';

  const postedDateDay = postedDateElement?.getElementsByTagName('day')[0];
  const day = postedDateDay?.textContent ?? '01';

  return new Date(`${year}-${month}-${day}`);
};

type GetServer = (doc: Document) => ArticleServer;

export const getServer: GetServer = flow(
  (doc) => {
    const doiDataElement = getElement(doc, 'doi_data');
    const resourceElement = doiDataElement?.getElementsByTagName('resource')[0];
    return resourceElement?.textContent;
  },
  O.fromNullable,
  O.filter((resource) => resource.includes('://medrxiv.org')),
  O.fold(
    constant('biorxiv'),
    constant('medrxiv'),
  ),
);

export const getAuthors = (doc: Document, doi: Doi, logger: Logger): Array<string> => {
  const contributorsElement = getElement(doc, 'contributors');

  if (!contributorsElement || typeof contributorsElement?.textContent !== 'string') {
    logger('debug', 'Did not find contributors', { doi });
    return [];
  }

  return Array.from(contributorsElement.getElementsByTagName('person_name'))
    .filter((person) => person.getAttribute('contributor_role') === 'author')
    .map((person) => {
      const givenName = person.getElementsByTagName('given_name')[0]?.textContent;
      // TODO: the decision as to what to display on error should live with th rendering component
      const surname = person.getElementsByTagName('surname')[0].textContent ?? 'Unknown author';

      if (!givenName) {
        return surname;
      }

      return `${givenName} ${surname}`;
    });
};
