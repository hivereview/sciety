import { XMLSerializer } from '@xmldom/xmldom';
import { load } from 'cheerio';
import { XMLParser } from 'fast-xml-parser';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow, identity, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { formatValidationErrors } from 'io-ts-reporters';
import * as tt from 'io-ts-types';
import { getElement } from './get-element';
import { ArticleAuthors } from '../../types/article-authors';
import { toHtmlFragment } from '../../types/html-fragment';
import { sanitise, SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';

const parser = new XMLParser({
  isArray: (name) => name === 'person_name',
  stopNodes: ['*.abstract', '*.given_name', '*.surname'],
  ignoreAttributes: (aName) => aName !== 'contributor_role',
});

const parseXmlDocument = (s: string) => E.tryCatch(
  () => parser.parse(s) as unknown,
  identity,
);

const personNameCodec = t.strict({
  given_name: tt.optionFromNullable(t.string),
  surname: t.string,
  '@_contributor_role': t.string,
});

const parsedCrossrefXmlCodec = t.strict({
  doi_records: t.strict({
    doi_record: t.strict({
      crossref: t.strict({
        posted_content: t.strict({
          abstract: tt.optionFromNullable(t.string),
          contributors: tt.optionFromNullable(
            t.strict({
              person_name: t.readonlyArray(personNameCodec),
              organization: tt.optionFromNullable(t.string),
            }),
          ),
        }),
      }),
    }),
  }),
});

export const getAbstract = (
  doc: Document,
  rawXmlString: string,
): O.Option<SanitisedHtmlFragment> => {
  const abstract = pipe(
    rawXmlString,
    parseXmlDocument,
    E.chainW(flow(
      parsedCrossrefXmlCodec.decode,
      E.mapLeft(formatValidationErrors),
      E.mapLeft((errors) => errors.join('\n')),
    )),
    E.map((parsed) => parsed.doi_records.doi_record.crossref.posted_content.abstract),
    E.chainW(E.fromOption(() => 'abstract field is undefined')),
  );

  if (E.isLeft(abstract)) {
    return O.none;
  }

  const transformXmlToHtml = (xml: string) => (
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

  const stripEmptySections = (html: string) => (
    html.replace(/<section>\s*<\/section>/g, '')
  );

  const removeSuperfluousTitles = (html: string) => {
    const model = load(html);
    model('h3').first().remove();
    model('h3:contains("Graphical abstract")').remove();
    return model.html();
  };

  return pipe(
    abstract.right,
    transformXmlToHtml,
    removeSuperfluousTitles,
    toHtmlFragment,
    sanitise,
    stripEmptySections,
    (output) => output.trim(),
    toHtmlFragment,
    sanitise,
    O.some,
  );
};

export const getTitle = (doc: Document): O.Option<SanitisedHtmlFragment> => {
  const titlesElement = getElement(doc, 'titles');
  const titleElement = titlesElement?.getElementsByTagName('title')[0];
  if (titleElement) {
    const title = new XMLSerializer()
      .serializeToString(titleElement)
      .replace(/^<title(?:.?)>([\s\S]*)<\/title>$/, '$1')
      .trim();
    return pipe(
      title,
      toHtmlFragment,
      sanitise,
      O.some,
    );
  }
  return O.none;
};

const constructAuthorName = (person: { given_name: O.Option<string>, surname: string }) => pipe(
  person.given_name,
  O.match(
    () => person.surname,
    (given) => `${given} ${person.surname}`,
  ),
);

export const getAuthors = (doc: Document, rawXmlString: string): ArticleAuthors => pipe(
  rawXmlString,
  parseXmlDocument,
  E.map((foo) => {
    console.log('!!!!!!!!!!!!!', JSON.stringify(foo));
    return foo;
  }),
  E.chainW(flow(
    parsedCrossrefXmlCodec.decode,
    E.mapLeft(formatValidationErrors),
    E.mapLeft((errors) => errors.join('\n')),
    E.mapLeft((foo) => { console.log('>>>>>>>>>>>>', rawXmlString, '\n>>', foo); return foo; }),
  )),
  O.fromEither,
  O.chain((parsed) => parsed.doi_records.doi_record.crossref.posted_content.contributors),
  O.map((contributors) => contributors.person_name),
  O.map(RA.filter((person) => person['@_contributor_role'] === 'author')),
  O.map(RA.map(constructAuthorName)),
  O.map(RA.map((name) => name.replace(/<[^>]*>/g, ''))),
);
