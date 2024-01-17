import * as t from 'io-ts';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import { CrossrefWork, crossrefWorkCodec } from './crossref-work';
import { Logger } from '../../../shared-ports';
import * as DE from '../../../types/data-error';
import { QueryCrossrefService } from './query-crossref-service';
import { decodeAndLogFailures } from '../../decode-and-log-failures';

const crossrefMultipleWorksResponseCodec = t.strict({
  message: t.strict({
    items: t.readonlyArray(crossrefWorkCodec),
  }),
}, 'crossrefMultipleWorksResponseCodec');

export const fetchWorksThatPointToIndividualWorks = (
  queryCrossrefService: QueryCrossrefService,
  logger: Logger,
) => (
  queue: ReadonlyArray<string>,
): TE.TaskEither<DE.DataError, ReadonlyArray<CrossrefWork>> => pipe(
  queue,
  TE.traverseArray((doi) => pipe(
    `https://api.crossref.org/works?filter=relation.object:${doi},type:posted-content,type:journal-article`,
    queryCrossrefService,
    TE.chainEitherKW((response) => pipe(
      response,
      decodeAndLogFailures(logger, crossrefMultipleWorksResponseCodec, { doi }),
      E.mapLeft(() => DE.unavailable),
    )),
  )),
  TE.map((responses) => pipe(
    responses,
    // eslint-disable-next-line fp-ts/prefer-chain
    RA.map((response) => response.message.items),
    RA.flatten,
  )),
);
