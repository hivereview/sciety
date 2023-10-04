import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { ViewModel } from '../view-model';
import { Dependencies } from '../dependencies';
import { GroupId } from '../../../types/group-id';
import { HtmlFragment } from '../../../types/html-fragment';
import { ArticleId } from '../../../types/article-id';

export type CurationStatement = {
  articleId: ArticleId,
  groupId: GroupId,
  quote: HtmlFragment,
  articleTitle: HtmlFragment,
};

export const constructCurationTeaser = (dependencies: Dependencies) => (curationStatement: CurationStatement): ViewModel['curationTeasers'][number] => pipe(
  curationStatement.groupId,
  dependencies.getGroup,
  O.match(
    () => {
      dependencies.logger('error', 'Group missing from readmodel', { groupId: curationStatement.groupId });
      return 'Curated by unknown';
    },
    (group) => `Curated by ${group.name}`,
  ),
  (caption) => ({
    ...curationStatement,
    caption,
    articleHref: `/articles/${curationStatement.articleId.value}`,
  }),
);
