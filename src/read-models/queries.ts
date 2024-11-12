import { annotations } from './annotations';
import { evaluatedArticlesLists } from './evaluated-articles-lists';
import { evaluations } from './evaluations';
import { evaluationsForNotifications } from './evaluations-for-notifications';
import { followings } from './followings';
import { groupActivity } from './group-activity';
import { groupAuthorisations } from './group-authorisations';
import { groups } from './groups';
import { idsOfEvalutedArticlesLists } from './ids-of-evaluated-articles-lists';
import { lists } from './lists';
import { queries as papersEvaluatedByGroupQueries } from './papers-evaluated-by-group';
import { users } from './users';

const queries = {
  ...annotations.queries,
  ...evaluations.queries,
  ...evaluatedArticlesLists.queries,
  ...evaluationsForNotifications.queries,
  ...followings.queries,
  ...groupActivity.queries,
  ...groupAuthorisations.queries,
  ...groups.queries,
  ...idsOfEvalutedArticlesLists.queries,
  ...lists.queries,
  ...users.queries,
  ...papersEvaluatedByGroupQueries,
};

export type Queries = { [K in keyof typeof queries]: ReturnType<typeof queries[K]> };
