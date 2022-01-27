import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow } from 'fp-ts/function';
import * as t from 'io-ts';
import { DateFromISOString } from 'io-ts-types';
import * as tt from 'io-ts-types';
import * as PR from 'io-ts/PathReporter';
import { DoiFromString } from '../types/codecs/DoiFromString';
import * as RI from '../types/review-id';

const readableEvaluations = t.readonlyArray(t.type({
  date: DateFromISOString,
  articleDoi: DoiFromString,
  evaluationLocator: RI.reviewIdCodec,
  authors: t.readonlyArray(t.string),
  publishedAt: DateFromISOString,
}));

export const decodeEvaluationsFromJsonl = flow(
  (fileContents: string) => fileContents.split('\n'),
  RA.filter((line) => line !== ''),
  E.traverseArray(tt.JsonFromString.decode),
  E.chain(readableEvaluations.decode),
  E.mapLeft(PR.failure),
);
