import { pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import { HandlePage } from '../../http/page-handler';
import { renderPage, toErrorPage } from './render-as-html';
import { Queries } from '../../shared-read-models';
import { constructViewModel } from './construct-view-model';

export const subscribeToListPage = (dependencies: Queries): HandlePage => (params: unknown) => pipe(
  params,
  constructViewModel(dependencies),
  TE.bimap(
    toErrorPage,
    renderPage,
  ),
  TE.map((content) => ({
    title: 'Subscribe to a list',
    content,
  })),
);
