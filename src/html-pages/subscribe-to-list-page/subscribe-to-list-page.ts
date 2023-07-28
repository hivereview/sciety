import { pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import * as t from 'io-ts';
import { HandlePage } from '../../http/page-handler';
import { toHtmlFragment } from '../../types/html-fragment';
import { ListId, listIdCodec } from '../../types/list-id';
import * as DE from '../../types/data-error';
import { Queries } from '../../shared-read-models';

const codec = t.strict({
  listId: listIdCodec,
});

type ViewModel = {
  listId: ListId,
  listName: string,
};

const renderAsHtml = (viewModel: ViewModel) => toHtmlFragment(`
  <header class="page-header">

    <h1>Subscribe to ${viewModel.listName}</h1>
  </header>

  <p class="subscribe-content">
    Please note that this is an experimental feature and may be a bit rough around the edges
    while we get things ship-shape.
  </p>

  <script type="text/javascript" src="https://form.jotform.com/jsform/232072517707050?listId=${viewModel.listId}"></script>
`);

export const subscribeToListPage = (dependencies: Queries): HandlePage => (params: unknown) => pipe(
  params,
  codec.decode,
  TE.fromEither,
  TE.chainW(({ listId }) => pipe(
    listId,
    dependencies.lookupList,
    TE.fromOption(() => DE.notFound),
  )),
  TE.map((list) => ({
    listId: list.id,
    listName: list.name,
  })),
  TE.bimap(
    () => ({
      type: DE.notFound,
      message: pipe('Something went wrong', toHtmlFragment),
    }),
    renderAsHtml,
  ),
  TE.map((content) => ({
    title: 'Subscribe to a list',
    content,
  })),
);
