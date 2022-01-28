import { sequenceS } from 'fp-ts/Apply';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { ArticleViewModel } from '.';
import { DomainEvent } from '../../domain-events';
import { getActivityForDoi } from '../../shared-read-models/article-activity';
import { ArticleAuthors } from '../../types/article-authors';
import { ArticleServer } from '../../types/article-server';
import * as DE from '../../types/data-error';
import { Doi } from '../../types/doi';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';

type ArticleItem = {
  articleId: Doi,
  server: ArticleServer,
  title: SanitisedHtmlFragment,
  authors: ArticleAuthors,
};

type GetLatestArticleVersionDate = (articleId: Doi, server: ArticleServer) => TO.TaskOption<Date>;

type Ports = {
  getLatestArticleVersionDate: GetLatestArticleVersionDate,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

export const populateArticleViewModel = (
  ports: Ports,
) => (item: ArticleItem): TE.TaskEither<DE.DataError, ArticleViewModel> => pipe(
  {
    latestVersionDate: ports.getLatestArticleVersionDate(item.articleId, item.server),
    articleActivity: pipe(
      ports.getAllEvents,
      T.map(getActivityForDoi(item.articleId)),
    ),
  },
  sequenceS(T.ApplyPar),
  T.map(({ latestVersionDate, articleActivity }) => ({
    ...item,
    latestVersionDate,
    latestActivityDate: articleActivity.latestActivityDate,
    evaluationCount: articleActivity.evaluationCount,
    listMembershipCount: articleActivity.listMembershipCount,
  })),
  TE.rightTask,
);
