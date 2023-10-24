import * as D from 'fp-ts/Date';
import * as E from 'fp-ts/Either';
import * as Eq from 'fp-ts/Eq';
import * as Ord from 'fp-ts/Ord';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow, pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import * as ER from './error-response';
import { Logger } from '../../shared-ports';
import * as DE from '../../types/data-error';
import * as AID from '../../types/article-id';
import * as GID from '../../types/group-id';
import { GroupId } from '../../types/group-id';
import { publisherAccountId } from '../docmap/publisher-account-id';
import { Queries } from '../../read-models';

export type DocmapIndexEntryModel = {
  articleId: AID.ArticleId,
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
  articleId: AID.eqArticleId,
  groupId: S.Eq,
});

export type Ports = Queries & {
  logger: Logger,
};

type IdentifyAllPossibleIndexEntries = (
  supportedGroups: ReadonlyArray<GroupId>,
  adapters: Ports,
) => E.Either<ER.ErrorResponse, ReadonlyArray<DocmapIndexEntryModel>>;

export const identifyAllPossibleIndexEntries: IdentifyAllPossibleIndexEntries = (
  supportedGroups,
  adapters,
) => pipe(
  supportedGroups,
  RA.chain(adapters.getEvaluationsByGroup),
  E.traverseArray((evaluation) => pipe(
    adapters.getGroup(evaluation.groupId),
    E.fromOption(() => {
      adapters.logger('error', 'docmap-index: a recorded evaluation refers to a non-existent group', { evaluation });
      return DE.notFound;
    }),
    E.map((group) => ({
      articleId: evaluation.articleId,
      groupId: evaluation.groupId,
      updated: evaluation.recordedAt,
      publisherAccountId: publisherAccountId(group),
    })),
  )),
  E.bimap(
    () => ER.internalServerError,
    flow(
      RA.sort(byDate),
      RA.uniq(eqEntry),
    ),
  ),
);
