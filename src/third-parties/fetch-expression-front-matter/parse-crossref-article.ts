import { load } from 'cheerio';
import { XMLParser } from 'fast-xml-parser';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow, identity, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { formatValidationErrors } from 'io-ts-reporters';
import * as tt from 'io-ts-types';
import { ArticleAuthors } from '../../types/article-authors';
import { toHtmlFragment } from '../../types/html-fragment';
import { sanitise, SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';

const parser = new XMLParser({
  transformTagName: (tagName) => ((['organization', 'person_name']).includes(tagName) ? '_org_or_person' : tagName),
  isArray: (tagNameOfItem) => tagNameOfItem === '_org_or_person',
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

const organizationCodec = t.strict({
  '#text': t.string,
  '@_contributor_role': t.string,
});

const orgOrPersonCodec = t.union([personNameCodec, organizationCodec]);

const parsedCrossrefXmlCodec = t.strict({
  doi_records: t.strict({
    doi_record: t.strict({
      crossref: t.strict({
        posted_content: t.strict({
          titles: t.strict({ title: t.string }),
          abstract: tt.optionFromNullable(t.string),
          contributors: tt.optionFromNullable(
            t.strict({
              _org_or_person: t.readonlyArray(orgOrPersonCodec),
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

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const getTitle = (doc: Document, rawXmlString: string): O.Option<SanitisedHtmlFragment> => pipe(
  rawXmlString,
  parseXmlDocument,
  E.map((foo) => { console.log('>>>>>>>>>>>>', JSON.stringify(foo)); return foo; }),
  E.chainW(flow(
    parsedCrossrefXmlCodec.decode,
    E.mapLeft(formatValidationErrors),
    E.mapLeft((errors) => errors.join('\n')),
  )),
  O.fromEither,
  O.map(() => sanitise(toHtmlFragment(''))),
)

/*
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
  */
;

type Person = {
  given_name: O.Option<string>,
  surname: string,
};

type Organization = {
  '#text': string,
};

const constructAuthorName = (author: Person | Organization) => {
  if ('given_name' in author) {
    return pipe(
      author.given_name,
      O.match(
        () => author.surname,
        (given) => `${given} ${author.surname}`,
      ),
    );
  }
  return author['#text'];
};

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
  O.map((contributors) => contributors._org_or_person),
  O.map(RA.filter((person) => person['@_contributor_role'] === 'author')),
  O.map(RA.map(constructAuthorName)),
  O.map(RA.map((name) => name.replace(/<[^>]*>/g, ''))),
);
