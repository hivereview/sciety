import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Params } from './construct-view-model/params';
import { renderAsHtml } from './render-as-html/render-as-html';
import { ErrorPageBodyViewModel } from '../../types/render-page-error';
import { HtmlPage } from '../html-page';
import { renderErrorPage } from './render-as-html/render-error-page';
import { Dependencies, constructViewModel } from './construct-view-model';

type SearchResultsPage = (
  dependencies: Dependencies,
) => (pageSize: number) => (params: Params) => TE.TaskEither<ErrorPageBodyViewModel, HtmlPage>;

export const searchResultsPage: SearchResultsPage = (dependencies) => (pageSize) => (params) => pipe(
  params,
  constructViewModel(dependencies, pageSize),
  TE.bimap(renderErrorPage, renderAsHtml),
);
