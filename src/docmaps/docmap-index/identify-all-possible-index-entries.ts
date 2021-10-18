import * as D from 'fp-ts/Date';
import * as Eq from 'fp-ts/Eq';
import * as Ord from 'fp-ts/Ord';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import * as ER from './error-response';
import { DomainEvent, isGroupEvaluatedArticleEvent } from '../../domain-events';
import * as DE from '../../types/data-error';
import * as Doi from '../../types/doi';
import { Group } from '../../types/group';
import * as GID from '../../types/group-id';
import { GroupId } from '../../types/group-id';
import { publisherAccountId } from '../docmap/publisher-account-id';

export type DocmapIndexEntryModel = {
  articleId: Doi.Doi,
  groupId: GID.GroupId,
  updated: Date,
  publisherAccountId: string,
};

const byDate: Ord.Ord<DocmapIndexEntryModel> = pipe(
  D.Ord,
  Ord.reverse,
  Ord.contramap((entry) => entry.updated),
);

const eqEntry: Eq.Eq<DocmapIndexEntryModel> = Eq.struct({
  articleId: Doi.eqDoi,
  groupId: S.Eq,
});

export type Ports = {
  getGroup: (groupId: GroupId) => TE.TaskEither<DE.DataError, Group>,
};

type IdentifyAllPossibleIndexEntries = (
  supportedGroups: ReadonlyArray<GroupId>,
  ports: Ports,
) => (events: ReadonlyArray<DomainEvent>) => TE.TaskEither<ER.ErrorResponse, ReadonlyArray<DocmapIndexEntryModel>>;

export const identifyAllPossibleIndexEntries: IdentifyAllPossibleIndexEntries = (
  supportedGroups,
  ports,
) => (
  events,
) => pipe(
  events,
  RA.filter(isGroupEvaluatedArticleEvent),
  RA.filter(({ groupId }) => supportedGroups.includes(groupId)),
  RA.map(({ articleId, groupId, date }) => ({
    articleId,
    groupId,
    updated: date,
  })),
  TE.traverseArray((incompleteEntry) => pipe(
    incompleteEntry.groupId,
    ports.getGroup,
    TE.map((group) => ({
      ...incompleteEntry,
      publisherAccountId: publisherAccountId(group),
    })),
  )),
  TE.bimap(
    () => ER.internalServerError,
    flow(
      RA.sort(byDate),
      RA.uniq(eqEntry),
    ),
  ),
);
