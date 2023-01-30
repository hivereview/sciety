import {
  $, goto, openBrowser,
} from 'taiko';
import { arbitraryGroupId } from '../../test/types/group-id.helper';
import * as RI from '../../src/types/review-id';
import {
  arbitraryDate, arbitraryString, arbitraryUri, arbitraryWord,
} from '../../test/helpers';
import { arbitraryReviewId } from '../../test/types/review-id.helper';
import { callApi } from '../call-api.helper';
import { screenshotTeardown } from '../utilities';
import { arbitraryDescriptionPath } from '../../test/types/description-path.helper';

describe('record an evaluation', () => {
  beforeEach(async () => {
    await openBrowser();
  });

  afterEach(screenshotTeardown);

  describe('when a new evaluation is successfully recorded', () => {
    const articleId = '10.1101/2021.07.23.453070';
    const evaluationLocator = RI.serialize(arbitraryReviewId());
    const groupId = arbitraryGroupId();

    beforeEach(async () => {
      await callApi('api/add-group', {
        groupId,
        name: arbitraryString(),
        shortDescription: arbitraryString(),
        homepage: arbitraryString(),
        avatarPath: arbitraryUri(),
        descriptionPath: arbitraryDescriptionPath(),
        slug: arbitraryWord(),
      });
      await callApi('api/record-evaluation', {
        evaluationLocator,
        articleId,
        groupId,
        publishedAt: arbitraryDate(),
        authors: [arbitraryString(), arbitraryString()],
      });
    });

    it('displays the evaluation', async () => {
      await goto(`localhost:8080/articles/${articleId}`);
      const evaluationIsDisplayed = await $(`[id="${evaluationLocator}"]`).exists();

      expect(evaluationIsDisplayed).toBe(true);
    });
  });
});
