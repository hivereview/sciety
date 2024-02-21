import * as E from 'fp-ts/Either';
import * as t from 'io-ts';
import { flow, identity, pipe } from 'fp-ts/function';
import { XMLParser } from 'fast-xml-parser';
import { formatValidationErrors } from 'io-ts-reporters';
import * as RA from 'fp-ts/ReadonlyArray';
import { SanitisedHtmlFragment, sanitise } from '../../types/sanitised-html-fragment';
import { toHtmlFragment } from '../../types/html-fragment';
import { AcmiJats, acmiJatsCodec } from './acmi-jats';
import * as AED from './acmi-evaluation-doi';

const parser = new XMLParser({
  isArray: (tagName) => tagName === 'sub-article',
});

const parseXmlDocument = (s: string) => E.tryCatch(
  () => parser.parse(s) as unknown,
  identity,
);

const hasBody = (subArticle: AcmiJats['article']['sub-article'][number]) => subArticle.body !== undefined;

export const toSubArticles = (
  input: unknown,
): E.Either<unknown, ReadonlyMap<AED.AcmiEvaluationDoi, SanitisedHtmlFragment>> => pipe(
  input,
  t.string.decode,
  E.chainW(parseXmlDocument),
  E.chainW(flow(
    acmiJatsCodec.decode,
    E.mapLeft(formatValidationErrors),
  )),
  E.map((acmiJats) => acmiJats.article['sub-article']),
  E.map(RA.filter(hasBody)),
  E.map(RA.match(
    () => new Map<AED.AcmiEvaluationDoi, SanitisedHtmlFragment>(),
    () => (new Map<AED.AcmiEvaluationDoi, SanitisedHtmlFragment>(
      [
        [
          AED.fromValidatedString(''),
          sanitise(toHtmlFragment('')),
        ],
      ],
    )),
  )),
);
