import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { evaluationRecorded, groupJoined } from '../../../src/domain-events';
import { populateArticleViewModel, Ports } from '../../../src/shared-components/article-card/populate-article-view-model';
import { toHtmlFragment } from '../../../src/types/html-fragment';
import { sanitise } from '../../../src/types/sanitised-html-fragment';
import { arbitraryDate } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('populate-article-view-model', () => {
  it('returns a correct view model', async () => {
    const group = arbitraryGroup();
    const articleId = arbitraryArticleId();
    const latestVersionDate = new Date();
    const earlierPublicationDate = new Date('1970');
    const laterPublicationDate = new Date('2020');
    const ports: Ports = {
      getAllEvents: T.of([
        groupJoined(
          group.id,
          group.name,
          group.avatarPath,
          group.descriptionPath,
          group.shortDescription,
          group.homepage,
          group.slug,
        ),
        evaluationRecorded(group.id, articleId, arbitraryReviewId(), [], laterPublicationDate, arbitraryDate()),
        evaluationRecorded(group.id, articleId, arbitraryReviewId(), [], earlierPublicationDate, arbitraryDate()),
      ]),
      getLatestArticleVersionDate: () => TO.some(latestVersionDate),
      getActivityForDoi: (a) => ({
        articleId: a,
        latestActivityDate: O.some(laterPublicationDate),
        evaluationCount: 2,
        listMembershipCount: 0,
      }),
    };

    const article = {
      articleId,
      server: 'biorxiv' as const,
      title: pipe('', toHtmlFragment, sanitise),
      authors: O.none,
    };
    const viewModel = await pipe(
      article,
      populateArticleViewModel(ports),
      TE.getOrElseW(() => T.of(shouldNotBeCalled)),
    )();

    expect(viewModel).toStrictEqual(expect.objectContaining({
      evaluationCount: 2,
      latestVersionDate: O.some(latestVersionDate),
      latestActivityDate: O.some(laterPublicationDate),
    }));
  });
});
