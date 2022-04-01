import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';

type UrlParams = {
  query: string,
  category: 'articles' | 'groups',
  evaluatedOnly: boolean,
  cursor: O.Option<string>,
};

const renderCursor = (cursor: O.Option<string>) => pipe(
  cursor,
  O.map(encodeURIComponent),
  O.fold(
    () => '',
    (c) => `&cursor=${c}`,
  ),
);

type BuildPageUrl = (urlParams: UrlParams) => string;

export const buildPageUrl: BuildPageUrl = ({
  query, category, evaluatedOnly, cursor,
}) => `/search?query=${encodeURIComponent(query)}&category=${category}${evaluatedOnly ? '&evaluatedOnly=true' : ''}${renderCursor(cursor)}`;
