import { AddArticleToList } from './add-article-to-list';
import { CommitEvents } from './commit-events';
import { CreateList } from './create-list';
import { EditListDetails } from './edit-list-details';
import { FetchArticle } from './fetch-article';
import { FetchReview } from './fetch-review';
import { FetchStaticFile } from './fetch-static-file';
import { GetAllEvents } from './get-all-events';
import { GetJson } from './get-json';
import { Logger } from './logger';
import { RecordSubjectArea } from './record-subject-area';
import { RemoveArticleFromList } from './remove-article-from-list';
import { FindVersionsForArticleDoi } from './find-versions-for-article-doi';

export { GetAllEvents } from './get-all-events';
export { CommitEvents } from './commit-events';
export { EditListDetails } from './edit-list-details';
export { GetJson } from './get-json';
export { Logger } from './logger';
export { AddArticleToList } from './add-article-to-list';
export { CreateList } from './create-list';
// ts-unused-exports:disable-next-line
export { FetchArticle } from './fetch-article';
export { FetchReview } from './fetch-review';
export { FetchStaticFile } from './fetch-static-file';
export { RecordSubjectArea } from './record-subject-area';
export { FindVersionsForArticleDoi } from './find-versions-for-article-doi';

export type SharedPorts = {
  addArticleToList: AddArticleToList,
  commitEvents: CommitEvents,
  createList: CreateList,
  editListDetails: EditListDetails,
  fetchArticle: FetchArticle,
  fetchReview: FetchReview,
  fetchStaticFile: FetchStaticFile,
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
  getAllEvents: GetAllEvents,
  getJson: GetJson,
  logger: Logger,
  recordSubjectArea: RecordSubjectArea,
  removeArticleFromList: RemoveArticleFromList,
};
