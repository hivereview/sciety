import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { DoiFromString } from '../types/codecs/DoiFromString';
import { EventIdFromString } from '../types/codecs/EventIdFromString';
import { UserIdFromString, UserId } from '../types/user-id';
import { Doi } from '../types/doi';
import { generate } from '../types/event-id';

export const userSavedArticleEventCodec = t.type({
  id: EventIdFromString,
  type: t.literal('UserSavedArticle'),
  date: tt.DateFromISOString,
  userId: UserIdFromString,
  articleId: DoiFromString,
});

export type UserSavedArticleEvent = t.TypeOf<typeof userSavedArticleEventCodec>;

export const isUserSavedArticleEvent = (event: { type: string }):
  event is UserSavedArticleEvent => event.type === 'UserSavedArticle';

export const userSavedArticle = (
  userId: UserId,
  doi: Doi,
  date: Date = new Date(),
): UserSavedArticleEvent => ({
  id: generate(),
  type: 'UserSavedArticle',
  date,
  userId,
  articleId: doi,
});
