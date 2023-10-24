import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import * as PR from 'io-ts/PathReporter';
import * as ER from './error-response';
import { DocmapIndexEntryModel } from './identify-all-possible-index-entries';
import { paramsCodec } from './params';

type FilterByParams = (
  query: Record<string, unknown>
) => (
  entries: ReadonlyArray<DocmapIndexEntryModel>
) => E.Either<ER.ErrorResponse, ReadonlyArray<DocmapIndexEntryModel>>;

const filterByPublisherAccount = (
  requestedPublisherAccountId: O.Option<string>,
) => (indexEntries: ReadonlyArray<DocmapIndexEntryModel>) => pipe(
  requestedPublisherAccountId,
  O.fold(
    () => indexEntries,
    (publisherAccountId) => pipe(
      indexEntries,
      RA.filter((indexEntry) => indexEntry.publisherAccountId === publisherAccountId),
    ),
  ),
);

const filterByUpdatedAfter = (
  updatedAfter: O.Option<Date>,
) => (docmaps: ReadonlyArray<DocmapIndexEntryModel>) => pipe(
  updatedAfter,
  O.fold(
    () => docmaps,
    (updated) => pipe(
      docmaps,
      RA.filter((docmap) => docmap.updated > updated),
    ),
  ),
);

const decodeParams = (query: unknown) => pipe(
  query,
  paramsCodec.decode,
  E.mapLeft(
    (errors) => pipe(
      PR.failure(errors).join('\n'),
      ER.badRequest,
    ),
  ),
);

export const filterByParams: FilterByParams = (query) => (entries) => pipe(
  query,
  decodeParams,
  E.map(({ publisheraccount, updatedAfter }) => pipe(
    entries,
    filterByUpdatedAfter(updatedAfter),
    filterByPublisherAccount(publisheraccount),
  )),
);
