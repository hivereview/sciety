import { NonEmptyString } from 'io-ts-types';
import * as uuid from 'uuid';
import { ArticleId } from '../types/article-id';

export type PaperIdThatIsADoi = string & { readonly PaperIdThatIsADoi: unique symbol };

export const getDoiPortion = (paperId: PaperIdThatIsADoi): string => paperId.split(':')[1];

export const toArticleId = (paperId: PaperIdThatIsADoi): ArticleId => new ArticleId(getDoiPortion(paperId));

export const fromArticleId = (articleId: ArticleId): PaperIdThatIsADoi => `doi:${articleId.value}` as PaperIdThatIsADoi;

export type PaperId = PaperIdThatIsADoi;

export const fromNonEmptyString = (candidate: NonEmptyString): PaperId => {
  if (uuid.validate(candidate)) {
    return `uuid:${candidate}` as PaperId;
  }
  return `doi:${candidate}` as PaperIdThatIsADoi;
};

export const isDoi = (paperId: PaperId): boolean => paperId.startsWith('doi:');

export const isUuid = (paperId: PaperId): boolean => paperId.startsWith('uuid:');
