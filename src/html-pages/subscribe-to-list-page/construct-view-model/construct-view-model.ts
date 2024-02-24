import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import * as t from 'io-ts';
import { listIdCodec } from '../../../types/list-id.js';
import { Queries } from '../../../read-models/index.js';
import { ViewModel } from '../view-model.js';
import * as DE from '../../../types/data-error.js';

const codec = t.strict({
  listId: listIdCodec,
});

type ConstructViewModel = (dependencies: Queries) => (params: unknown) => TE.TaskEither<DE.DataError, ViewModel>;

export const constructViewModel: ConstructViewModel = (dependencies) => (params) => pipe(
  params,
  codec.decode,
  E.mapLeft(() => DE.notFound),
  TE.fromEither,
  TE.chain(({ listId }) => pipe(
    listId,
    dependencies.lookupList,
    TE.fromOption(() => DE.notFound),
  )),
  TE.map((list) => ({
    listId: list.id,
    listName: list.name,
    listHref: `/lists/${list.id}`,
    pageHeading: `Subscribe to ${list.name}`,
  })),
);
