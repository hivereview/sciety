import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Dependencies } from './dependencies';
import * as DE from '../../../../types/data-error';
import { constructCategoryPageHref } from '../../../paths';
import { ViewModel } from '../view-model';

const constructBrowseByCategory = (dependencies: Dependencies) => pipe(
  dependencies.fetchSearchCategories(),
  TE.map(RA.map((title) => ({
    title,
    href: constructCategoryPageHref(title),
  }))),
  T.map(O.fromEither),
);

export const constructViewModel = (dependencies: Dependencies): TE.TaskEither<DE.DataError, ViewModel> => pipe(
  constructBrowseByCategory(dependencies),
  T.map((browseByCategory) => ({ browseByCategory })),
  TE.rightTask,
);
