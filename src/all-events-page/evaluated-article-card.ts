import { htmlEscape } from 'escape-goat';
import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { constant, pipe } from 'fp-ts/function';
import { GroupEvaluatedArticleEvent } from '../domain-events';
import { templateDate } from '../shared-components/date';
import * as DE from '../types/data-error';
import { Doi } from '../types/doi';
import { Group } from '../types/group';
import { GroupId } from '../types/group-id';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

type GetGroup = (id: GroupId) => TO.TaskOption<Group>;

type FetchArticle = (doi: Doi) => TE.TaskEither<DE.DataError, {
  title: HtmlFragment,
  authors: ReadonlyArray<string>,
}>;

export type EvaluatedArticleCard = {
  groupId: GroupId,
  articleId: Doi,
  evaluationCount: number,
  date: Date,
};

export const evaluatedArticleCard = (
  getGroup: GetGroup,
  fetchArticle: FetchArticle,
) => (
  event: EvaluatedArticleCard | GroupEvaluatedArticleEvent,
): TE.TaskEither<DE.DataError, HtmlFragment> => pipe(
  {
    group: pipe(
      event.groupId,
      getGroup,
      T.map(E.fromOption(constant(DE.unavailable))),
    ),
    article: pipe(
      event.articleId,
      fetchArticle,
    ),
  },
  sequenceS(TE.ApplyPar),
  TE.map(({ group, article }) => ({
    group,
    article,
    authors: pipe(
      article.authors,
      RA.map((author) => `<li class="article-card__author">${htmlEscape(author)}</li>`),
      (authorListItems) => `
        <ol class="article-card__authors" role="list">
          ${authorListItems.join('')}
        </ol>
      `,
      toHtmlFragment,
    ),
  })),
  TE.map(({ group, article, authors }) => `
    <article class="all-events-card">
      <img src="${group.avatarPath}" alt="" width="36" height="36">
      <span>${group.name} evaluated an article. ${templateDate(event.date)}</span>
      ${article.title}
      ${authors}
    </article>
  `),
  TE.map(toHtmlFragment),
);
