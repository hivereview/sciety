import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import {
  fetchExtraDetails,
  Ports as FetchExtraDetailsPorts,
} from './fetch-extra-details';
import { Params, performAllSearches, Ports as PerformAllSearchesPorts } from './perform-all-searches';
import { renderErrorPage, RenderPage, renderPage } from './render-page';
import { selectSubsetToDisplay } from './select-subset-to-display';
import { Ports as GetArticleVersionDatePorts, getLatestArticleVersionDate } from '../../shared-components/article-card';

// ts-unused-exports:disable-next-line
export type Ports = PerformAllSearchesPorts
// The next two lines are necessary as getLatestVersionDate is not in CollectedPorts and is constructed locally
& Omit<FetchExtraDetailsPorts, 'getLatestArticleVersionDate'>
& GetArticleVersionDatePorts;

type SearchResultsPage = (ports: Ports) => (pageSize: number) => (params: Params) => ReturnType<RenderPage>;

export const searchResultsPage: SearchResultsPage = (ports) => (pageSize) => (params) => pipe(
  params,
  performAllSearches(ports)(pageSize),
  TE.map(selectSubsetToDisplay),
  TE.chainTaskK(fetchExtraDetails({
    ...ports,
    getLatestArticleVersionDate: getLatestArticleVersionDate(ports),
  })),
  TE.bimap(renderErrorPage, renderPage),
);
