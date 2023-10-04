import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import { constructArticleCard } from '../../../shared-components/article-card';
import { ArticleId } from '../../../types/article-id';
import { ViewModel } from '../view-model';
import { Dependencies } from './dependencies';

export const constructRelatedArticles = (
  doi: ArticleId, dependencies: Dependencies,
): T.Task<ViewModel['relatedArticles']> => pipe(
  dependencies.fetchRelatedArticles(doi),
  TE.map(RA.takeLeft(3)),
  TE.chainW(TE.traverseArray((recommendedPaper) => pipe(
    recommendedPaper.articleId,
    constructArticleCard(dependencies),
  ))),
  TO.fromTaskEither,
);
