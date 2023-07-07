export type Evaluation = {
  date: Date,
  articleDoi: string,
  evaluationLocator: string,
  authors: ReadonlyArray<string>,
  evaluationType?: string,
};

export type Evaluations = ReadonlyArray<Evaluation>;
