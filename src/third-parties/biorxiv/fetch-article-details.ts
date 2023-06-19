import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import { formatValidationErrors } from 'io-ts-reporters';
import { SupportedArticleServer } from './article-server-with-version-information';
import {
  biorxivDetailsApiResponse, ResponseWithVersions, responseWithVersions,
} from './biorxiv-details-api-response';
import { Doi } from '../../types/doi';
import { Foo } from '../query-external-service';
import { Logger } from '../../shared-ports';

type Dependencies = {
  queryExternalService: Foo,
  logger: Logger,
};

const constructUrl = (doi: Doi, server: SupportedArticleServer) => (
  `https://api.biorxiv.org/details/${server}/${doi.value}`
);

type FetchArticleDetails = ({ queryExternalService, logger }: Dependencies, doi: Doi, server: SupportedArticleServer)
=> TE.TaskEither<void, ResponseWithVersions>;

export const fetchArticleDetails: FetchArticleDetails = ({ queryExternalService, logger }, doi, server) => pipe(
  constructUrl(doi, server),
  queryExternalService(logger, 24 * 60 * 60),
  TE.chainEitherKW(flow(
    biorxivDetailsApiResponse.decode,
    E.mapLeft((errors) => logger('error', 'Failed to parse biorxiv response', {
      errors: formatValidationErrors(errors).join('\n'),
      url: constructUrl(doi, server),
    })),
  )),
  TE.filterOrElseW(responseWithVersions.is, () => {
    logger('warn', 'No versions found on biorxiv/medrxiv', { doi, server });
  }),
  TE.mapLeft(() => undefined),
);
