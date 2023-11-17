import { SearchForArticles } from './search-for-articles.js';
import { AddArticleToList } from './add-article-to-list.js';
import { CommitEvents } from './commit-events.js';
import { CreateList } from './create-list.js';
import { EditListDetails } from './edit-list-details.js';
import { FetchArticle } from './fetch-article.js';
import { FetchReview } from './fetch-review.js';
import { FetchStaticFile } from './fetch-static-file.js';
import { GetAllEvents } from './get-all-events.js';
import { GetArticleSubjectArea } from './get-article-subject-area.js';
import { Logger } from './logger.js';
import { RecordSubjectArea } from './record-subject-area.js';
import { RemoveArticleFromList } from './remove-article-from-list.js';
import { FindVersionsForArticleDoi } from './find-versions-for-article-doi.js';
import { FetchRelatedArticles } from './fetch-related-articles.js';

export { GetAllEvents } from './get-all-events.js';
export { CommitEvents } from './commit-events.js';
export { EditListDetails } from './edit-list-details.js';
export { Logger } from './logger.js';
export { AddArticleToList } from './add-article-to-list.js';
export { CreateList } from './create-list.js';
export { FetchArticle } from './fetch-article.js';
export { FetchReview } from './fetch-review.js';
export { FetchStaticFile } from './fetch-static-file.js';
export { GetArticleSubjectArea } from './get-article-subject-area.js';
export { RecordSubjectArea } from './record-subject-area.js';
export { SearchForArticles } from './search-for-articles.js';
export { FindVersionsForArticleDoi } from './find-versions-for-article-doi.js';
export { FetchRelatedArticles } from './fetch-related-articles.js';

export type SharedPorts = {
  addArticleToList: AddArticleToList,
  commitEvents: CommitEvents,
  createList: CreateList,
  editListDetails: EditListDetails,
  fetchArticle: FetchArticle,
  fetchRelatedArticles: FetchRelatedArticles,
  fetchReview: FetchReview,
  fetchStaticFile: FetchStaticFile,
  findVersionsForArticleDoi: FindVersionsForArticleDoi,
  getAllEvents: GetAllEvents,
  getArticleSubjectArea: GetArticleSubjectArea,
  logger: Logger,
  recordSubjectArea: RecordSubjectArea,
  removeArticleFromList: RemoveArticleFromList,
  searchForArticles: SearchForArticles,
};
