import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { pipe, tupled } from 'fp-ts/function';
import * as DE from '../../../types/data-error';
import { fetchExtraDetails } from './fetch-extra-details';
import { ViewModel } from '../view-model';
import { Dependencies } from './dependencies';
import { Params } from './params';

export const constructViewModel = (
  dependencies: Dependencies,
  pageSize: number,
) => (params: Params): TE.TaskEither<DE.DataError, ViewModel> => pipe(
  [
    params.query,
    params.cursor,
    params.evaluatedOnly,
  ],
  tupled(dependencies.searchForPaperExpressions(pageSize)),
  TE.map((searchResults) => ({
    evaluatedOnly: params.evaluatedOnly,
    itemsToDisplay: searchResults.items,
    query: params.query,
    nextCursor: searchResults.nextCursor,
    pageNumber: O.getOrElse(() => 1)(params.page),
    numberOfPages: Math.ceil(searchResults.total / pageSize),
  })),
  TE.chainTaskK(fetchExtraDetails(dependencies)),
);
