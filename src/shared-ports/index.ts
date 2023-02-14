import { SearchForArticles } from './search-for-articles';
import { AddArticleToList } from './add-article-to-list';
import { CommitEvents } from './commit-events';
import { CreateList } from './create-list';
import { EditListDetails } from './edit-list-details';
import { FetchArticle } from './fetch-article';
import { FetchReview } from './fetch-review';
import { FetchStaticFile } from './fetch-static-file';
import { GetAllEvents } from './get-all-events';
import { GetAllGroups } from './get-all-groups';
import { GetArticleIdsByState } from './get-article-ids-by-state';
import { GetArticleSubjectArea } from './get-article-subject-area';
import { GetEvaluatedArticlesListIdForGroup } from './get-evaluated-articles-list-id-for-group';
import { GetFollowers } from './get-followers';
import { GetGroup } from './get-group';
import { GetGroupBySlug } from './get-group-by-slug';
import { GetGroupsFollowedBy } from './get-groups-followed-by';
import { GetList } from './get-list';
import { GetOneArticleIdInEvaluatedState } from './get-one-article-id-in-evaluated-state';
import { GetOneArticleReadyToBeListed } from './get-one-article-ready-to-be-listed';
import { LookupUser } from './lookup-user';
import { LookupUserByHandle } from './lookup-user-by-handle';
import { IsArticleOnTheListOwnedBy } from './is-article-on-the-list-owned-by';
import { IsFollowing } from './is-following';
import { Logger } from './logger';
import { RecordSubjectArea } from './record-subject-area';
import { RemoveArticleFromList } from './remove-article-from-list';
import { SelectAllListsOwnedBy } from './select-all-lists-owned-by';
import { GetActivityForDoi } from './get-activity-for-doi';
import { GetActivityForDois } from './get-activity-for-dois';

export { GetAllEvents } from './get-all-events';
export { CommitEvents } from './commit-events';
export { EditListDetails } from './edit-list-details';
export { Logger } from './logger';
export { AddArticleToList } from './add-article-to-list';
export { CreateList } from './create-list';
export { FetchArticle } from './fetch-article';
export { FetchReview } from './fetch-review';
export { FetchStaticFile } from './fetch-static-file';
export { GetGroup } from './get-group';
export { GetGroupBySlug } from './get-group-by-slug';
export { GetAllGroups } from './get-all-groups';
export { GetList } from './get-list';
export { LookupUser } from './lookup-user';
export { LookupUserByHandle } from './lookup-user-by-handle';
// ts-unused-exports:disable-next-line
export { RemoveArticleFromList } from './remove-article-from-list';
export { GetArticleSubjectArea } from './get-article-subject-area';
export { RecordSubjectArea } from './record-subject-area';
export { GetArticleIdsByState, ArticleIdsByState } from './get-article-ids-by-state';
export { IsArticleOnTheListOwnedBy } from './is-article-on-the-list-owned-by';
export { SelectAllListsOwnedBy } from './select-all-lists-owned-by';
export { GetOneArticleReadyToBeListed, ArticleWithSubjectArea } from './get-one-article-ready-to-be-listed';
export { GetOneArticleIdInEvaluatedState } from './get-one-article-id-in-evaluated-state';
// ts-unused-exports:disable-next-line
export { GetEvaluatedArticlesListIdForGroup } from './get-evaluated-articles-list-id-for-group';
export { SearchForArticles } from './search-for-articles';
export { GetFollowers } from './get-followers';
export { GetGroupsFollowedBy } from './get-groups-followed-by';
export { IsFollowing } from './is-following';
export { GetActivityForDoi } from './get-activity-for-doi';
export { GetActivityForDois } from './get-activity-for-dois';

export type SharedPorts = {
  addArticleToList: AddArticleToList,
  commitEvents: CommitEvents,
  createList: CreateList,
  editListDetails: EditListDetails,
  fetchArticle: FetchArticle,
  fetchReview: FetchReview,
  fetchStaticFile: FetchStaticFile,
  getAllEvents: GetAllEvents,
  getAllGroups: GetAllGroups,
  getArticleIdsByState: GetArticleIdsByState,
  getArticleSubjectArea: GetArticleSubjectArea,
  getFollowers: GetFollowers,
  getGroup: GetGroup,
  getGroupBySlug: GetGroupBySlug,
  getGroupsFollowedBy: GetGroupsFollowedBy,
  getList: GetList,
  lookupUser: LookupUser,
  lookupUserByHandle: LookupUserByHandle,
  getOneArticleIdInEvaluatedState: GetOneArticleIdInEvaluatedState,
  getOneArticleReadyToBeListed: GetOneArticleReadyToBeListed,
  isArticleOnTheListOwnedBy: IsArticleOnTheListOwnedBy,
  isFollowing: IsFollowing,
  logger: Logger,
  recordSubjectArea: RecordSubjectArea,
  removeArticleFromList: RemoveArticleFromList,
  selectAllListsOwnedBy: SelectAllListsOwnedBy,
  getEvaluatedArticlesListIdForGroup: GetEvaluatedArticlesListIdForGroup,
  searchForArticles: SearchForArticles,
  getActivityForDoi: GetActivityForDoi,
  getActivityForDois: GetActivityForDois,
};
