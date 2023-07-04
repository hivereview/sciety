import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { DomainEvent } from '../domain-events';
import { articleActivity } from './article-activity';
import { Queries } from './queries';
import { evaluations } from './evaluations';
import { InitialisedReadModel, UnionToIntersection } from './initialised-read-model';
import { annotations } from './annotations';
import { followings } from './followings';
import { groupActivity } from './group-activity';
import { groups } from './groups';
import { idsOfEvalutedArticlesLists } from './ids-of-evaluated-articles-lists';
import { lists } from './lists';
import { users } from './users';
import { addArticleToElifeSubjectAreaList } from '../add-article-to-elife-subject-area-list/read-model';

type DispatchToAllReadModels = (events: ReadonlyArray<DomainEvent>) => void;

type Dispatcher = {
  queries: Queries,
  dispatchToAllReadModels: DispatchToAllReadModels,
};

export const dispatcher = (): Dispatcher => {
  const initialisedReadModels = [
    new InitialisedReadModel(addArticleToElifeSubjectAreaList),
    new InitialisedReadModel(annotations),
    new InitialisedReadModel(articleActivity),
    new InitialisedReadModel(evaluations),
    new InitialisedReadModel(followings),
    new InitialisedReadModel(groupActivity),
    new InitialisedReadModel(groups),
    new InitialisedReadModel(idsOfEvalutedArticlesLists),
    new InitialisedReadModel(lists),
    new InitialisedReadModel(users),
  ];

  const dispatchToAllReadModels: DispatchToAllReadModels = (events) => {
    pipe(
      initialisedReadModels,
      RA.map((readModel) => readModel.dispatch(events)),
    );
  };

  const queries = pipe(
    initialisedReadModels,
    RA.map((readModel) => readModel.queries),
    (arrayOfQueries) => arrayOfQueries.reduce(
      (collectedQueries, query) => ({ ...collectedQueries, ...query }),
    ) as UnionToIntersection<typeof arrayOfQueries[number]>,
  );

  return {
    queries,
    dispatchToAllReadModels,
  };
};
