import { URL } from 'url';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { constant, flow, pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { EvaluationFetcher } from './fetch-review';
import { Logger } from './logger';
import * as DE from '../types/data-error';
import { toHtmlFragment } from '../types/html-fragment';
import { sanitise } from '../types/sanitised-html-fragment';

type GetHtml = (url: string) => TE.TaskEither<DE.DataError, string>;

const summary = (logger: Logger) => (doc: Document) => pipe(
  doc.querySelector('meta[name=description]')?.getAttribute('content'),
  O.fromNullable,
  E.fromOption(() => DE.notFound),
  E.bimap(
    (err) => {
      logger('error', 'Rapid-review summary has no description');
      return err;
    },
    flow(
      (text) => text.replace(/•/g, '<br>'),
      (description) => `
      <h3>Strength of evidence</h3>
      <p>${description}</p>
      `,
    ),
  ),
);

const review = (doc: Document) => pipe(
  {
    creator: O.fromNullable(Array.from(doc.querySelectorAll('meta[name="dc.creator"]')).map((creatorNode) => creatorNode.getAttribute('content')).join(', ')),
    title: O.fromNullable(doc.querySelector('meta[name="dc.title"]')?.getAttribute('content')),
    description: O.fromNullable(doc.querySelector('meta[name=description]')?.getAttribute('content')),
  },
  ({ description, creator, title }) => `
    ${pipe(creator, O.fold(constant(''), (txt) => `<h3>${txt}</h3>`))}
    ${pipe(title, O.fold(constant(''), (txt) => `<p>${txt}</p>`))}
    ${pipe(description, O.fold(constant(''), (txt) => `<p>${txt}</p>`))}
  `,
  E.right,
);

const extractEvaluation = (logger: Logger) => (doc: Document) => {
  if (doc.querySelector('meta[name="dc.title"]')?.getAttribute('content')?.startsWith('Reviews of ')) {
    return summary(logger)(doc);
  }
  return review(doc);
};

export const fetchRapidReview = (logger: Logger, getHtml: GetHtml): EvaluationFetcher => (key) => pipe(
  key,
  getHtml,
  TE.chainEitherKW(flow(
    (html) => new JSDOM(html).window.document,
    extractEvaluation(logger),
    E.map(toHtmlFragment),
    E.map(sanitise),
  )),
  TE.map((fullText) => ({
    fullText,
    url: new URL(key),
  })),
);
