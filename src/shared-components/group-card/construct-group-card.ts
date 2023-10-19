import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import * as DE from '../../types/data-error';
import { GroupId } from '../../types/group-id';
import { toHtmlFragment } from '../../types/html-fragment';
import * as LOID from '../../types/list-owner-id';
import { sanitise } from '../../types/sanitised-html-fragment';
import { Queries } from '../../read-models';
import { GroupCardViewModel } from './view-model';
import { RecordedEvaluation } from '../../types/recorded-evaluation';

const isCurationStatement = (
  recordedEvaluation: RecordedEvaluation,
) => {
  if (O.isNone(recordedEvaluation.type)) {
    return false;
  }
  return recordedEvaluation.type.value === 'curation-statement';
};

const calculateCuratedArticlesCount = (groupId: GroupId, queries: Queries) => pipe(
  groupId,
  queries.getEvaluationsByGroup,
  RA.filter(isCurationStatement),
  RA.map((curationStatement) => curationStatement.articleId.value),
  RA.uniq(S.Eq),
  RA.size,
);

const calculateListCount = (groupId: GroupId, queries: Queries) => pipe(
  groupId,
  LOID.fromGroupId,
  queries.selectAllListsOwnedBy,
  RA.size,
);

export const constructGroupCard = (
  queries: Queries,
) => (
  groupId: GroupId,
): E.Either<DE.DataError, GroupCardViewModel> => pipe(
  queries.getGroup(groupId),
  O.chain((group) => pipe(
    group.id,
    queries.getActivityForGroup,
    O.map((activity) => ({
      ...group,
      ...activity,
      followerCount: queries.getFollowers(groupId).length,
      description: pipe(group.shortDescription, toHtmlFragment, sanitise),
      curatedArticlesCount: calculateCuratedArticlesCount(groupId, queries),
      listCount: calculateListCount(groupId, queries),
    })),
  )),
  E.fromOption(() => DE.notFound),
);
