/* eslint-disable no-loops/no-loops */
import axios from 'axios';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Evaluation } from './evaluations';
import { fetchData } from './fetch-data';
import { FetchEvaluations } from './update-all';

type BiorxivItem = {
  biorxiv_doi: string,
  published_doi: string,
};

type BiorxivResponse = {
  messages: Array<{
    cursor: number | string,
    count: number,
    total: number,
  }>,
  collection: Array<BiorxivItem>,
};

type CrossrefResponse = {
  message: {
    items: [
      {
        DOI: string,
        'published-print': {
          'date-parts': [
            [number, number, number],
          ],
        },
      },
    ],
  },
};

const getReviews = (reviewDoiPrefix: string) => (biorxivItem: BiorxivItem) => async () => {
  const result: Array<Evaluation> = [];
  const publishedDoi = biorxivItem.published_doi;
  const biorxivDoi = biorxivItem.biorxiv_doi;
  const headers: Record<string, string> = {
    'User-Agent': 'Sciety (http://sciety.org; mailto:team@sciety.org)',
  };
  if (process.env.CROSSREF_API_BEARER_TOKEN !== undefined) {
    headers['Crossref-Plus-API-Token'] = `Bearer ${process.env.CROSSREF_API_BEARER_TOKEN}`;
  }
  const { data } = await axios.get<CrossrefResponse>(
    `https://api.crossref.org/prefixes/${reviewDoiPrefix}/works?rows=1000&filter=type:peer-review,relation.object:${publishedDoi}`,
    { headers },
  );
  data.message.items.forEach((item) => {
    const [year, month, day] = item['published-print']['date-parts'][0];
    const date = new Date(year, month - 1, day);
    const reviewDoi = item.DOI;
    result.push({
      date,
      articleDoi: biorxivDoi,
      evaluationLocator: `doi:${reviewDoi}`,
    });
  });
  return result;
};

const fetchPage = (reviewDoiPrefix: string, baseUrl: string, offset: number) => pipe(
  fetchData<BiorxivResponse>(`${baseUrl}/${offset}`),
  TE.fold(
    (error) => { console.log(error); return T.of([]); },
    (data) => T.of(data.collection),
  ),
  T.chain(T.traverseArray(getReviews(reviewDoiPrefix))),
  T.map(RA.flatten),
);

const identifyCandidates = (
  doiPrefix: string,
  reviewDoiPrefix: string,
) => async (): Promise<ReadonlyArray<Evaluation>> => {
  const startDate = new Date(Date.now() - (60 * 24 * 60 * 60 * 1000)).toISOString().split('T')[0];
  const today = new Date().toISOString().split('T')[0];
  const result: Array<Evaluation> = [];
  let offset = 0;
  do {
    const baseUrl = `https://api.biorxiv.org/publisher/${doiPrefix}/${startDate}/${today}`;
    const reviews = await fetchPage(reviewDoiPrefix, baseUrl, offset)();
    if (reviews.length === 0) {
      return result;
    }
    result.concat(reviews);
    offset += reviews.length;
  } while (offset >= 0);
  return result;
};

export const fetchReviewsFromCrossrefViaBiorxiv = (
  doiPrefix: string,
  reviewDoiPrefix: string,
): FetchEvaluations => () => pipe(
  identifyCandidates(doiPrefix, reviewDoiPrefix),
  T.map((evaluations) => ({
    evaluations,
    skippedItems: [],
  })),
  TE.rightTask,
);
