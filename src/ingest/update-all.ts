import fs from 'fs';
import chalk from 'chalk';
import { printf } from 'fast-printf';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as Es from './evaluations';
import { fetchData, FetchData } from './fetch-data';
import { fetchGoogleSheet, FetchGoogleSheet } from './fetch-google-sheet';

type Adapters = {
  fetchData: FetchData,
  fetchGoogleSheet: FetchGoogleSheet,
};

export type FetchEvaluations = (adapters: Adapters) => TE.TaskEither<string, Es.Evaluations>;

export type Group = {
  id: string,
  name: string,
  fetchFeed: FetchEvaluations,
};

const writeFile = (path: string) => (contents: string) => TE.taskify(fs.writeFile)(path, contents);

const overwriteCsv = (group: Group) => (evaluations: Es.Evaluations) => pipe(
  `./data/reviews/${group.id}.csv`,
  Es.fromFile,
  TE.map((existing) => pipe(
    [...existing, ...evaluations],
    Es.uniq,
    (all) => ({
      all,
      existing,
    }),
  )),
  TE.chain((results) => pipe(
    results.all,
    Es.toCsv,
    writeFile(`./data/reviews/${group.id}.csv`),
    TE.bimap(
      (error) => error.toString(),
      () => ({
        total: results.all.length,
        added: results.all.length - results.existing.length,
      }),
    ),
  )),
);

const report = (group: Group) => (message: string) => {
  process.stderr.write(printf('%-36s %s\n', chalk.white(group.name), message));
};

const reportError = (group: Group) => (message: string) => pipe(
  chalk.redBright(message),
  report(group),
);

type Results = {
  total: number,
  added: number,
};

const reportSuccess = (group: Group) => (results: Results) => pipe(
  process.env.INGEST_LOG,
  O.fromNullable,
  O.filter((v) => v === 'DEBUG'),
  O.fold(
    () => printf('%5d evaluations (%d new)', results.total, results.added),
    () => printf('%5d evaluations (%s, %s existing, %s)',
      results.total,
      chalk.green(`${results.added} new`),
      chalk.white(results.total - results.added),
      chalk.yellow(`${0} skipped`)),
  ),
  report(group),
);

const updateGroup = (group: Group): T.Task<void> => pipe(
  group.fetchFeed({
    fetchData,
    fetchGoogleSheet,
  }),
  TE.chain(overwriteCsv(group)),
  TE.match(
    reportError(group),
    reportSuccess(group),
  ),
);

export const updateAll = (groups: ReadonlyArray<Group>): T.Task<ReadonlyArray<void>> => pipe(
  groups,
  T.traverseArray(updateGroup),
);
