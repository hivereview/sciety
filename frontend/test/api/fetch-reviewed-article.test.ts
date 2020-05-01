import { FetchArticle } from '../../src/api/fetch-article';
import { FetchReview } from '../../src/api/fetch-review';
import createFetchReviewedArticle from '../../src/api/fetch-reviewed-article';
import Doi from '../../src/data/doi';
import ReviewReferenceRepository from '../../src/types/review-reference-repository';
import shouldNotBeCalled from '../should-not-be-called';

const articleDoi = new Doi('10.5555/12345678');
const reviewDoi = new Doi('10.5555/987654321');

describe('fetch-reviewed-article', (): void => {
  const reviewReferenceRepository: ReviewReferenceRepository = {
    add: shouldNotBeCalled,
    findReviewDoisForArticleDoi: () => [reviewDoi],
  };

  const emptyReviewReferenceRepository: ReviewReferenceRepository = {
    add: shouldNotBeCalled,
    findReviewDoisForArticleDoi: () => [],
  };

  const fetchArticle: FetchArticle = async (doi) => ({
    title: 'Article title',
    authors: ['Josiah S. Carberry', 'Albert Einstein'],
    publicationDate: new Date('2010-02-01'),
    abstract: 'Article abstract.',
    doi,
  });

  const fetchReview: FetchReview = async (doi) => ({
    author: 'John Doe',
    publicationDate: new Date('2010-02-01'),
    summary: 'Pretty good.',
    doi,
  });

  it('includes the article', async () => {
    const fetchReviewedArticle = createFetchReviewedArticle(reviewReferenceRepository, fetchArticle, fetchReview);
    const reviewedArticle = await fetchReviewedArticle(articleDoi);

    expect(reviewedArticle.article.doi).toBe(articleDoi);
  });

  it('includes the reviews', async () => {
    const fetchReviewedArticle = createFetchReviewedArticle(reviewReferenceRepository, fetchArticle, fetchReview);
    const reviewedArticle = await fetchReviewedArticle(articleDoi);

    expect(reviewedArticle.reviews).toHaveLength(1);
    expect(reviewedArticle.reviews[0].doi).toBe(reviewDoi);
  });

  it('may not have reviews', async () => {
    const fetchReviewedArticle = createFetchReviewedArticle(emptyReviewReferenceRepository, fetchArticle,
      shouldNotBeCalled);
    const reviewedArticle = await fetchReviewedArticle(articleDoi);

    expect(reviewedArticle.reviews).toHaveLength(0);
  });
});
