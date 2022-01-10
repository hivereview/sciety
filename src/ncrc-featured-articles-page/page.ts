import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as R from 'fp-ts/Record';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { articlesList, Ports } from './articles-list/articlesList';
import { lists } from './lists';
import { renderComponent } from '../list-page/header/render-component';
import { renderErrorPage } from '../list-page/render-page';
import * as DE from '../types/data-error';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { ListId } from '../types/list-id';

const headers = (listId: ListId) => pipe(
  {
    'cbd478fe-3ff7-4125-ac9f-c94ff52ae0f7': {
      name: 'High interest articles',
      description: 'Articles that have been identified as high interest by NCRC editors.',
      ownerName: 'NCRC',
      ownerHref: '/groups/ncrc',
      ownerAvatarPath: '/static/groups/ncrc--62f9b0d0-8d43-4766-a52a-ce02af61bc6a.jpg',
      articleCount: lists['cbd478fe-3ff7-4125-ac9f-c94ff52ae0f7'].length,
      lastUpdated: O.some(new Date('2021-11-24')),
    },
    '5ac3a439-e5c6-4b15-b109-92928a740812': {
      name: 'Endorsed articles',
      description: 'Articles that have been endorsed by Biophysics Colab.',
      ownerName: 'Biophysics Colab',
      ownerHref: '/groups/biophysics-colab',
      ownerAvatarPath: '/static/groups/biophysics-colab--4bbf0c12-629b-4bb8-91d6-974f4df8efb2.png',
      articleCount: lists['5ac3a439-e5c6-4b15-b109-92928a740812'].length,
      lastUpdated: O.some(new Date('2021-11-22T15:09:00Z')),
    },
  },
  R.lookup(listId),
  E.fromOption(() => DE.notFound),
);

type Components = {
  header: HtmlFragment,
  articlesList: HtmlFragment,
  title: string,
};

const renderPage = (components: Components) => ({
  content: toHtmlFragment(`
    ${components.header}
    <section class="evaluated-articles">
      ${components.articlesList}
    </section>
  `),
  title: components.title,
});

export const paramsCodec = t.type({
  page: tt.withFallback(tt.NumberFromString, 1),
  id: t.string,
});

type Params = t.TypeOf<typeof paramsCodec>;

export const page = (ports: Ports) => (params: Params): TE.TaskEither<RenderPageError, Page> => pipe(
  {
    header: pipe(
      headers(params.id),
      E.map(renderComponent),
      T.of,
    ),
    articlesList: articlesList(ports, params.id, params.page),
    title: pipe(
      headers(params.id),
      E.map((header) => header.name),
      T.of,
    ),
  },
  sequenceS(TE.ApplyPar),
  TE.bimap(renderErrorPage, renderPage),
);
