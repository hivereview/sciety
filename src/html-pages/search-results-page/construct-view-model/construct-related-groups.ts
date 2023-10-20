import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { ArticleId } from '../../../types/article-id';
import { ViewModel } from '../view-model';
import { Dependencies } from './dependencies';
import * as GID from '../../../types/group-id';
import { constructGroupLink } from '../../../shared-components/group-link';

export const constructRelatedGroups = (dependencies: Dependencies) => (articleIds: ReadonlyArray<ArticleId>): ViewModel['relatedGroups'] => pipe(
  articleIds,
  RA.flatMap(dependencies.getEvaluationsForArticle),
  RA.map((recordedEvaluation) => recordedEvaluation.groupId),
  RA.uniq(GID.eq),
  RA.map(constructGroupLink(dependencies)),
  RA.compact,
  RA.matchW(
    () => ({ tag: 'no-groups-evaluated-the-found-articles' as const }),
    (groupLinkWithLogoViewModels) => ({
      tag: 'some-related-groups' as const,
      items: groupLinkWithLogoViewModels,
    }),
  ),
);
