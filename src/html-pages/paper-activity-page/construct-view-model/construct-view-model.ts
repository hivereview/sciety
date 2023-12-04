import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { sequenceS } from 'fp-ts/Apply';
import * as RA from 'fp-ts/ReadonlyArray';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import * as O from 'fp-ts/Option';
import { feedSummary } from './feed-summary';
import { getArticleFeedEventsByDateDescending } from './get-article-feed-events';
import * as DE from '../../../types/data-error';
import { ViewModel } from '../view-model';
import { userIdCodec } from '../../../types/user-id';
import { constructListedIn } from './construct-listed-in';
import { constructUserListManagement } from './construct-user-list-management';
import { constructRelatedArticles } from './construct-related-articles';
import { detectLanguage } from '../../../shared-components/lang-attribute';
import { constructCurationStatements } from '../../../shared-components/curation-statements';
import { Dependencies } from './dependencies';
import { constructReviewingGroups } from '../../../shared-components/reviewing-groups';
import { PaperExpressionLocator, PaperId } from '../../../third-parties';
import { sanitise } from '../../../types/sanitised-html-fragment';
import { toHtmlFragment } from '../../../types/html-fragment';
import { ArticleId } from '../../../types/article-id';

export const paramsCodec = t.type({
  candidatePaperId: tt.NonEmptyString,
  user: tt.optionFromNullable(t.type({ id: userIdCodec })),
});

type Params = t.TypeOf<typeof paramsCodec>;

const toFullArticleUrl = (paperId: PaperId.PaperId) => `https://doi.org/${PaperId.getDoiPortion(paperId)}`;

type ConstructViewModel = (dependencies: Dependencies) => (params: Params) => TE.TaskEither<DE.DataError, ViewModel>;

const getFrontMatterForMostRecentExpression = (dependencies: Dependencies) => (paperId: PaperId.PaperId): ReturnType<Dependencies['fetchPaperExpressionFrontMatter']> => {
  if (PaperId.isDoi(paperId)) {
    return pipe(
      paperId,
      PaperId.getDoiPortion,
      PaperExpressionLocator.fromDoi,
      dependencies.fetchPaperExpressionFrontMatter,
    );
  }
  if (PaperId.isUuid(paperId)) {
    return TE.right({
      abstract: sanitise(toHtmlFragment('An abstract')),
      authors: O.some(['Author']),
      doi: new ArticleId('10.9999/1234'),
      title: sanitise(toHtmlFragment('The title')),
      server: 'microbiologyresearch',
    });
  }
  return TE.left(DE.notFound);
};

export const constructViewModel: ConstructViewModel = (dependencies) => (params) => pipe(
  PaperId.fromNonEmptyString(params.candidatePaperId),
  getFrontMatterForMostRecentExpression(dependencies),
  TE.chainW((frontMatter) => pipe(
    {
      feedItemsByDateDescending: (
        getArticleFeedEventsByDateDescending(dependencies)(
          PaperId.fromNonEmptyString(params.candidatePaperId),
          frontMatter.server,
        )
      ),
      relatedArticles: constructRelatedArticles(frontMatter.doi, dependencies),
      curationStatements: constructCurationStatements(dependencies, frontMatter.doi),
    },
    sequenceS(T.ApplyPar),
    TE.rightTask,
    TE.map(({ curationStatements, feedItemsByDateDescending, relatedArticles }) => ({
      ...frontMatter,
      titleLanguageCode: detectLanguage(frontMatter.title),
      abstractLanguageCode: detectLanguage(frontMatter.abstract),
      userListManagement: constructUserListManagement(params.user, dependencies, frontMatter.doi),
      fullArticleUrl: pipe(
        PaperId.fromNonEmptyString(params.candidatePaperId),
        toFullArticleUrl,
      ),
      feedItemsByDateDescending,
      ...feedSummary(feedItemsByDateDescending),
      listedIn: constructListedIn(dependencies)(frontMatter.doi),
      relatedArticles,
      curationStatements: pipe(
        curationStatements,
        RA.map((curationStatementWithGroupAndContent) => ({
          ...curationStatementWithGroupAndContent,
          fullText: curationStatementWithGroupAndContent.statement,
          fullTextLanguageCode: curationStatementWithGroupAndContent.statementLanguageCode,
        })),
      ),
      reviewingGroups: constructReviewingGroups(dependencies, frontMatter.doi),
    })),
  )),
);
