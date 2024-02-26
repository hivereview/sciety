import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { URL } from 'url';
import { pipe } from 'fp-ts/function';
import * as DE from '../../types/data-error';
import { EvaluationFetcher } from '../evaluation-fetcher';
import { QueryExternalService } from '../query-external-service';
import { deriveFullTextsOfEvaluations, lookupFullText } from './derive-full-texts-of-evaluations';
import { Logger } from '../../shared-ports';
import { toJatsXmlUrlOfPublisher } from './to-jats-xml-url-of-publisher';
import * as AED from './acmi-evaluation-doi';

const fetchEvaluationFromPublisherJatsXmlEndpoint = (
  queryExternalService: QueryExternalService,
  logger: Logger,
) => (acmiEvaluationDoi: AED.AcmiEvaluationDoi) => pipe(
  acmiEvaluationDoi,
  toJatsXmlUrlOfPublisher,
  TE.fromOption(() => DE.unavailable),
  TE.chain(queryExternalService()),
  TE.chainEitherK(deriveFullTextsOfEvaluations(logger)),
  TE.chainEitherKW(lookupFullText(acmiEvaluationDoi)),
  TE.map((fullText) => ({
    url: new URL(`https://doi.org/${acmiEvaluationDoi}`),
    fullText,
  })),

);

export const fetchAccessMicrobiologyEvaluation = (
  queryExternalService: QueryExternalService,
  logger: Logger,
): EvaluationFetcher => (key: string) => pipe(
  key,
  AED.acmiEvaluationDoiCodec.decode,
  E.mapLeft(() => DE.unavailable),
  TE.fromEither,
  TE.chain(fetchEvaluationFromPublisherJatsXmlEndpoint(queryExternalService, logger)),
);
