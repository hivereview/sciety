import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { constant, flow, pipe } from 'fp-ts/function';
import { templateListItems } from '../shared-components';
import { Doi } from '../types/doi';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

export type SavedArticle = {
  doi: Doi,
  title: O.Option<HtmlFragment>,
};

type RenderAsLink = (savedArticle: SavedArticle) => HtmlFragment;

type RenderSavedArticles = (savedArticles: ReadonlyArray<SavedArticle>) => HtmlFragment;

const renderAsLink: RenderAsLink = flow(
  (item) => ({
    doi: item.doi,
    title: pipe(item.title, O.getOrElse(constant(toHtmlFragment('an article')))),
  }),
  (item) => `<a href="/articles/${item.doi.value}" class="saved-articles__link">${item.title}</a>`,
  toHtmlFragment,
);

export const renderSavedArticles: RenderSavedArticles = flow(
  RA.map(renderAsLink),
  RNEA.fromReadonlyArray,
  O.map((items) => templateListItems(items, 'saved-articles__item')),
  O.fold(
    () => '',
    (list) => `
        <section id="saved-articles">
          <h2>Saved articles</h2>
          <ol class="saved-articles" role="list">
            ${list}
          </ol>
        </section>
      `,
  ),
  toHtmlFragment,
);
