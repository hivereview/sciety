import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { DomainEvent } from '../../domain-events';
import { GroupIdFromString } from '../../types/codecs/GroupIdFromString';
import * as Doi from '../../types/doi';
import * as GID from '../../types/group-id';
import { GroupId } from '../../types/group-id';
import { allDocmapDois } from '../all-docmap-dois';

export const paramsCodec = t.type({
  updatedAfter: tt.optionFromNullable(tt.DateFromISOString),
  group: tt.optionFromNullable(GroupIdFromString),
});

type Params = t.TypeOf<typeof paramsCodec>;

type Ports = {
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

const filterByGroup = (
  selectedGroup: O.Option<GID.GroupId>,
) => (docmaps: ReadonlyArray<{ articleId: Doi.Doi, groupId: GroupId, updated: Date }>) => pipe(
  selectedGroup,
  O.fold(
    () => docmaps,
    (groupId) => pipe(
      docmaps,
      RA.filter((docmap) => docmap.groupId === groupId),
    ),
  ),
);

const filterByUpdatedAfter = (
  updatedAfter: O.Option<Date>,
) => (docmaps: ReadonlyArray<{ articleId: Doi.Doi, updated: Date }>) => pipe(
  updatedAfter,
  O.fold(
    () => docmaps,
    (updated) => pipe(
      docmaps,
      RA.filter((docmap) => docmap.updated > updated),
    ),
  ),
);

const articlesEvaluatedByGroup = (ports: Ports) => (params: Params) => pipe(
  ports.getAllEvents,
  T.map(flow(
    allDocmapDois,
    filterByGroup(params.group),
    filterByUpdatedAfter(params.updatedAfter),
  )),
);

export const generateDocmapDois = (
  ports: Ports,
) => (params: Params): TE.TaskEither<never, ReadonlyArray<Doi.Doi>> => pipe(
  params,
  articlesEvaluatedByGroup(ports),
  T.map(RA.map(({ articleId }) => articleId)),
  T.map(E.right),
);
