import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { GroupViewModel } from './render-group-card';
import { updateGroupMeta } from './update-group-meta';
import { DomainEvent } from '../../domain-events';
import { getGroup } from '../../shared-read-models/groups';
import { List } from '../../shared-read-models/lists';
import * as DE from '../../types/data-error';
import { GroupId } from '../../types/group-id';
import { toHtmlFragment } from '../../types/html-fragment';
import { sanitise } from '../../types/sanitised-html-fragment';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

export type Ports = {
  getAllEvents: GetAllEvents,
  getListsOwnedBy: (groupId: GroupId) => TE.TaskEither<DE.DataError, ReadonlyArray<List>>,
};

export const populateGroupViewModel = (
  ports: Ports,
) => (
  groupId: GroupId,
): TE.TaskEither<DE.DataError, GroupViewModel> => pipe(
  ports.getAllEvents,
  T.map(getGroup(groupId)),
  TE.chainTaskK((group) => pipe(
    ports.getAllEvents,
    T.map(RA.reduce({ evaluationCount: 0, followerCount: 0, latestActivity: O.none }, updateGroupMeta(group.id))),
    T.map((meta) => ({
      ...group,
      ...meta,
      description: pipe(group.shortDescription, toHtmlFragment, sanitise),
    })),
  )),
  TE.chain((partial) => pipe(
    groupId,
    ports.getListsOwnedBy,
    TE.map((lists) => lists.length),
    TE.map((listCount) => ({
      ...partial,
      listCount,
    })),
  )),
);
