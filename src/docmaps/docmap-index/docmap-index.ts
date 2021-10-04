import { URL } from 'url';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import { generateDocmapDois, Ports as GenerateDocmapDoisPorts, paramsCodec } from './generate-docmap-dois';
import * as GID from '../../types/group-id';
import { docmap, Ports as DocmapPorts } from '../docmap/docmap';

type Ports = DocmapPorts & GenerateDocmapDoisPorts;

const ncrcGroupId = GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a');

type DocmapIndex = (ports: Ports) => (query: unknown) => T.Task<{
  body: {
    articles: ReadonlyArray<unknown>,
  },
  status: StatusCodes,
}>;

export const docmapIndex: DocmapIndex = (ports) => flow(
  paramsCodec.decode,
  TE.fromEither,
  TE.chainW(generateDocmapDois(ports)),
  TE.chainW(TE.traverseArray(docmap({
    ...ports,
    fetchReview: () => TE.right({
      url: new URL(`https://example.com/source-url-of-evaluation-${Math.random()}`),
    }),
  }, ncrcGroupId))),
  TE.matchW(
    () => ({
      body: { articles: [] },
      status: StatusCodes.INTERNAL_SERVER_ERROR,
    }),
    (docmaps) => ({
      body: { articles: docmaps },
      status: StatusCodes.OK,
    }),
  ),
);
