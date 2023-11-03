import * as O from 'fp-ts/Option';
import { ArticleId } from '../../types/article-id';
import { HtmlFragment } from '../../types/html-fragment';
import { ListId } from '../../types/list-id';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';

export type UnrecoverableError = 'article-not-in-list';

export type ViewModel = {
  articleId: ArticleId,
  listId: ListId,
  articleTitle: SanitisedHtmlFragment,
  listName: string,
  pageHeading: HtmlFragment,
  unrecoverableError: O.Option<UnrecoverableError>,
};
