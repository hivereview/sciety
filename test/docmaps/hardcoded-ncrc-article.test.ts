import * as T from 'fp-ts/Task';
import * as TO from 'fp-ts/TaskOption';
import { hardcodedNcrcArticle } from '../../src/docmaps/hardcoded-ncrc-article';
import * as GroupId from '../../src/types/group-id';
import { arbitraryDate, arbitraryString, arbitraryUri } from '../helpers';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';

describe('hardcoded-ncrc-article', () => {
  it('includes the article id', async () => {
    const articleId = arbitraryDoi();
    const ports = {
      findReviewsForArticleDoi: () => T.of(
        [
          {
            reviewId: arbitraryReviewId(),
            groupId: GroupId.fromValidatedString('316db7d9-88cc-4c26-b386-f067e0f56334'),
            occurredAt: arbitraryDate(),
          },
        ],
      ),
      getGroup: () => TO.none,
    };
    const docmap = await hardcodedNcrcArticle(ports)(articleId.value)();

    expect(docmap).toStrictEqual(expect.objectContaining({
      id: expect.stringContaining(articleId.value),
    }));
  });

  it('includes the publisher properties', async () => {
    const groupId = arbitraryGroupId();
    const homepage = arbitraryUri();
    const avatarPath = arbitraryString();
    const ports = {
      findReviewsForArticleDoi: () => T.of(
        [
          {
            reviewId: arbitraryReviewId(),
            groupId,
            occurredAt: arbitraryDate(),
          },
        ],
      ),
      getGroup: () => TO.some({
        id: groupId,
        homepage,
        avatarPath,
        shortDescription: arbitraryString(),
        descriptionPath: arbitraryString(),
        name: arbitraryString(),
      }),
    };
    const docmap = await hardcodedNcrcArticle(ports)(arbitraryDoi().value)();

    expect(docmap).toStrictEqual(expect.objectContaining({
      publisher: {
        id: homepage,
        logo: expect.stringContaining(avatarPath),
        homepage,
        account: {
          id: expect.stringContaining(groupId),
          service: 'https://sciety.org',
        },
      },
    }));
  });

  it('includes the uri and doi in the inputs to the first step', async () => {
    const articleId = arbitraryDoi().value;
    const ports = {
      findReviewsForArticleDoi: () => T.of(
        [
          {
            reviewId: arbitraryReviewId(),
            groupId: arbitraryGroupId(),
            occurredAt: arbitraryDate(),
          },
        ],
      ),
      getGroup: () => TO.none,
    };
    const docmap = await hardcodedNcrcArticle(ports)(articleId)();

    expect(docmap).toStrictEqual(expect.objectContaining({
      steps: expect.objectContaining({
        '_:b0': expect.objectContaining({
          inputs: [expect.objectContaining(
            {
              doi: articleId,
              url: expect.stringContaining(articleId),
            },
          )],
        }),
      }),
    }));
  });
});
