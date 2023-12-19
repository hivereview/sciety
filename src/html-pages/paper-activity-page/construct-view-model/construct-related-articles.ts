import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import * as EDOI from '../../../types/expression-doi';
import { constructPaperActivitySummaryCard } from '../../../shared-components/paper-activity-summary-card';
import { ArticleId } from '../../../types/article-id';
import { ViewModel } from '../view-model';
import { Dependencies } from './dependencies';

export const constructRelatedArticles = (
  doi: ArticleId, dependencies: Dependencies,
): T.Task<ViewModel['relatedArticles']> => pipe(
  dependencies.fetchRelatedArticles(doi),
  TE.map(RA.takeLeft(3)),
  TE.chainW(TE.traverseArray((recommendedPaper) => pipe(
    EDOI.fromValidatedString(recommendedPaper.articleId.value),
    constructPaperActivitySummaryCard(dependencies),
  ))),
  TO.fromTaskEither,
);
