import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { populateArticleActivities, Ports as PopulateArticleActivitiesPorts } from './populate-article-activities';
import { ContentWithPaginationViewModel } from './render-content-with-pagination';
import { toPageOfCards, Ports as ToPageOfCardsPorts } from './to-page-of-cards';
import { paginate } from '../../../shared-components/paginate';
import * as DE from '../../../types/data-error';
import { Doi } from '../../../types/doi';
import { ListId } from '../../../types/list-id';
import { ListOwnerId } from '../../../types/list-owner-id';

export type Ports = PopulateArticleActivitiesPorts & ToPageOfCardsPorts;

export const constructContentWithPaginationViewModel = (
  ports: Ports,
  listId: ListId,
  pageNumber: number,
  editCapability: boolean,
  listOwnerId: ListOwnerId,
) => (articleIds: ReadonlyArray<Doi>): TE.TaskEither<DE.DataError | 'no-articles-can-be-fetched', ContentWithPaginationViewModel> => pipe(
  articleIds,
  paginate(20, pageNumber),
  E.map(populateArticleActivities(ports)),
  TE.fromEither,
  TE.chainW((pageOfArticles) => pipe(
    pageOfArticles,
    toPageOfCards(ports, editCapability, listId, listOwnerId),
    TE.map((articles) => ({ articles, pagination: pageOfArticles })),
  )),
);
