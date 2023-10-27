import { ExternalQueries } from './external-queries';

export type EvaluationFetcher = (key: string) => ReturnType<ExternalQueries['fetchReview']>;
