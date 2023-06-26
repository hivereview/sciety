import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { constructArticleCardViewModel, Ports as ConstructArticleCardViewModelPorts } from './construct-article-card-view-model';
import { ArticleErrorCardViewModel } from './render-article-error-card';
import { ListId } from '../../types/list-id';
import { Doi } from '../../types/doi';
import { ArticleCardWithControlsAndAnnotationViewModel } from './render-article-card-with-controls-and-annotation';
import { ArticleCardViewModel } from './render-article-card';
import { Queries } from '../../shared-read-models';

export type Ports = ConstructArticleCardViewModelPorts & Queries;

const toArticleCardWithControlsAndAnnotationViewModel = (
  ports: Ports,
  editCapability: boolean,
  listId: ListId,
) => (articleCard: ArticleCardViewModel) => pipe(
  {
    articleCard,
    annotationContent: ports.getAnnotationContent(listId, articleCard.articleId),
    hasControls: editCapability,
    listId,
  },
);

export const constructArticleCardWithControlsAndAnnotationViewModel = (
  ports: Ports,
  editCapability: boolean,
  listId: ListId,
) => (
  articleId: Doi,
): TE.TaskEither<ArticleErrorCardViewModel, ArticleCardWithControlsAndAnnotationViewModel> => pipe(
  articleId,
  constructArticleCardViewModel(ports),
  TE.map(toArticleCardWithControlsAndAnnotationViewModel(ports, editCapability, listId)),
);
