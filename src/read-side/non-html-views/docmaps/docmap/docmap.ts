import * as E from 'fp-ts/Either';
import { Json } from 'fp-ts/Json';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import * as t from 'io-ts';
import { Ports, generateDocmaps } from './generate-docmaps';
import { NonHtmlViewError } from '../../non-html-view-error';
import { NonHtmlViewParams } from '../../non-html-view-params';

const paramsCodec = t.strict({
  doi: t.string,
});

export const docmap = (
  ports: Ports,
) => (
  params: NonHtmlViewParams,
): TE.TaskEither<NonHtmlViewError, Json> => pipe(
  params,
  paramsCodec.decode,
  E.mapLeft(() => ({ status: StatusCodes.BAD_REQUEST, message: 'Cannot understand the request' })),
  TE.fromEither,
  TE.chain((decodedParams) => generateDocmaps(ports)(decodedParams.doi)),
);
