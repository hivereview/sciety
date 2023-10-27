import * as TE from 'fp-ts/TaskEither';
import { URL } from 'url';
import { XMLBuilder, XMLParser } from 'fast-xml-parser';
import * as t from 'io-ts';
import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import { accessMicrobiologyXmlResponse000569 } from './acmi.0.000569.v1';
import { Logger } from '../../shared-ports';
import * as DE from '../../types/data-error';
import { EvaluationFetcher } from '../evaluation-fetcher';
import { toHtmlFragment } from '../../types/html-fragment';
import { sanitise } from '../../types/sanitised-html-fragment';
import { accessMicrobiologyXmlResponse000530 } from './acmi.0.000530.v1';

const parser = new XMLParser({});
const builder = new XMLBuilder();

const xmlCodec = t.strict({
  article: t.strict({
    'sub-article': t.readonlyArray(
      t.strict({
        'front-stub': t.strict({
          'article-id': t.string,
        }),
        body: t.unknown,
      }),
    ),
  }),
});

const parseXml = (accessMicrobiologyXmlResponse: string) => pipe(
  parser.parse(accessMicrobiologyXmlResponse),
  xmlCodec.decode,
  E.mapLeft(() => DE.unavailable),
);

export const fetchAccessMicrobiologyEvaluation = (logger: Logger): EvaluationFetcher => (key: string) => {
  logger('debug', 'calling fetchAccessMicrobiology', { key });
  if (key === '10.1099/acmi.0.000530.v1.3') {
    return pipe(
      parseXml(accessMicrobiologyXmlResponse000530),
      E.map((response) => builder.build(response.article['sub-article'][2].body).toString() as string),
      E.map((text) => ({
        url: new URL(`https://doi.org/${key}`),
        fullText: sanitise(toHtmlFragment(text)),
      })),
      TE.fromEither,
    );
  }
  if (key === '10.1099/acmi.0.000569.v1.4') {
    return pipe(
      parseXml(accessMicrobiologyXmlResponse000569),
      E.map((response) => builder.build(response.article['sub-article'][3].body).toString() as string),
      E.map((text) => ({
        url: new URL(`https://doi.org/${key}`),
        fullText: sanitise(toHtmlFragment(text)),
      })),
      TE.fromEither,
    );
  }
  return TE.left(DE.unavailable);
};
