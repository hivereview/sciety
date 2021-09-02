import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Evaluation } from './evaluations';
import { FetchData } from './fetch-data';
import { FetchEvaluations, SkippedItem } from './update-all';

type Row = {
  id: string,
  created: string,
  uri: string,
};

type HypothesisResponse = {
  total: number,
  rows: Array<Row>,
};

// TODO bioRxiv/medRxiv content is available at multiple URL patterns:
// curl "https://api.hypothes.is/api/search?uri.parts=biorxiv&limit=100" | jq --raw-output ".rows[].target[].source"

const toEvaluation = (row: Row): E.Either<SkippedItem, Evaluation> => {
  const shortRegex = '((?:[^%"#?\\s])+)';
  const matches = new RegExp(`https?://(?:www.)?(bio|med)rxiv.org/cgi/content/(?:10.1101|short)/${shortRegex}$`).exec(row.uri);
  if (matches === null) {
    return E.left({ item: row.uri, reason: 'Cannot parse into the biorxiv DOI' });
  }
  const doi = matches[2];
  return E.right({
    date: new Date(row.created),
    articleDoi: `10.1101/${doi}`,
    evaluationLocator: `hypothesis:${row.id}`,
  });
};

// ts-unused-exports:disable-next-line
export const processServer = (
  userId: string,
  getData: FetchData,
) => (server: string) => async (): Promise<ReadonlyArray<Row>> => {
  let result: ReadonlyArray<Row> = [];
  const perPage = 200;
  let latestDate = encodeURIComponent(new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString());
  let rows;

  // eslint-disable-next-line no-loops/no-loops
  do {
    const url = `https://api.hypothes.is/api/search?user=${userId}&uri.parts=${server}&limit=${perPage}&sort=created&order=asc&search_after=${latestDate}`;
    rows = await pipe(
      getData<HypothesisResponse>(url),
      TE.fold(
        () => T.of([]),
        (response) => T.of(response.rows),
      ),
    )();
    if (rows.length === 0) {
      return result;
    }
    result = [...result, ...rows];
    latestDate = encodeURIComponent(rows[rows.length - 1].created);
  } while (rows.length > 0);
  return result;
};

type Ports = {
  fetchData: FetchData,
};

export const fetchReviewsFromHypothesisUser = (publisherUserId: string): FetchEvaluations => (ports: Ports) => pipe(
  ['biorxiv', 'medrxiv'],
  T.traverseArray(processServer(publisherUserId, ports.fetchData)),
  T.map(RA.flatten),
  T.map(RA.map(toEvaluation)),
  T.map((parts) => ({
    evaluations: RA.rights(parts),
    skippedItems: RA.lefts(parts),
  })),
  TE.rightTask,
);
