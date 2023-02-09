import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { constructViewModel, Params, Ports } from './construct-view-model';
import { renderAsHtml, toErrorPage } from './render-as-html';
import { Page } from '../../types/page';
import { RenderPageError } from '../../types/render-page-error';

// ts-unused-exports:disable-next-line
export { Ports } from './construct-view-model';

type ArticlePage = (ports: Ports) => (params: Params) => TE.TaskEither<RenderPageError, Page>;

// ts-unused-exports:disable-next-line
export const articlePage: ArticlePage = (ports) => (params) => pipe(
  params,
  constructViewModel(ports),
  TE.bimap(toErrorPage, renderAsHtml),
);
