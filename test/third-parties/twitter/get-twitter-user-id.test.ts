import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { GetTwitterResponse } from '../../../src/third-parties/twitter/get-twitter-response';
import { getTwitterUserId } from '../../../src/third-parties/twitter/get-twitter-user-id';
import * as DE from '../../../src/types/data-error';
import { dummyLogger } from '../../dummy-logger';
import { arbitraryWord } from '../../helpers';
import { axiosError } from '../../infrastructure/helpers';
import { arbitraryUserId } from '../../types/user-id.helper';

describe('get-twitter-user-id', () => {
  describe('when the user handle exists', () => {
    it('returns a UserId', async () => {
      const userId = arbitraryUserId();
      const getTwitterResponse: GetTwitterResponse = () => TE.right({
        data: {
          id: userId.toString(),
          name: 'Giorgio Sironi 🇮🇹🇬🇧🇪🇺',
          username: 'giorgiosironi',
        },
      });
      const result = await getTwitterUserId(getTwitterResponse, dummyLogger)(arbitraryWord())();

      expect(result).toStrictEqual(E.right(userId));
    });
  });

  describe('when the user handle does not exist', () => {
    it('returns not found', async () => {
      const getTwitterResponse: GetTwitterResponse = () => TE.right({
        errors: [
          {
            value: 'giggggfbgfb',
            detail: 'Could not find user with username: [giggggfbgfb].',
            title: 'Not Found Error',
            resource_type: 'user',
            parameter: 'username',
            resource_id: 'giggggfbgfb',
            type: 'https://api.twitter.com/2/problems/resource-not-found',
          },
        ],
      });
      const result = await getTwitterUserId(getTwitterResponse, dummyLogger)(arbitraryWord())();

      expect(result).toStrictEqual(E.left(DE.notFound));
    });
  });

  describe('when the request fails', () => {
    it('returns unavailable', async () => {
      const getTwitterResponse: GetTwitterResponse = () => TE.left(axiosError(500));
      const result = await getTwitterUserId(getTwitterResponse, dummyLogger)(arbitraryWord())();

      expect(result).toStrictEqual(E.left(DE.unavailable));
    });
  });
});
