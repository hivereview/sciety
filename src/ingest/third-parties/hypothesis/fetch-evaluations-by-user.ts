import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { Annotation } from '../../../third-parties/hypothesis/annotation';
import { processServer } from './process-server';
import { FetchData } from '../../fetch-data';

export const fetchEvaluationsByUserSince = (
  startDate: Date,
  fetchData: FetchData,
) => (userId: string): TE.TaskEither<string, ReadonlyArray<Annotation>> => pipe(
  processServer(`user=${userId}`, startDate, fetchData),
);
