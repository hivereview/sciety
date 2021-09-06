import * as crypto from 'crypto';
import * as fs from 'fs';
import * as E from 'fp-ts/Either';
import * as Json from 'fp-ts/Json';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RM from 'fp-ts/ReadonlyMap';
import * as S from 'fp-ts/string';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';

const logEntryFromJson = t.type({
  http_user_agent: t.string,
  request: t.string,
  remote_addr: t.string,
  time_local: tt.DateFromISOString,
});

type LogEntry = t.TypeOf<typeof logEntryFromJson>;

const logsFromJson = t.array(logEntryFromJson);

type Logs = t.TypeOf<typeof logsFromJson>;

type PageView = {
  time_local: Date,
  request: string,
};

type ObfuscatedPageView = PageView & {
  visitorId: string,
};

type Visits = Map<string, ReadonlyArray<PageView>>;

const collectPageViewsForVisitor = (accum: Visits, pageView: ObfuscatedPageView): Visits => {
  const pvs = accum.get(pageView.visitorId) || [];
  return accum.set(pageView.visitorId, pvs.concat({
    time_local: pageView.time_local,
    request: pageView.request,
  }));
};

const isNotCrawler = (pageViews: ReadonlyArray<PageView>) => pipe(
  pageViews,
  RA.every((v) => !v.request.match(/\/robots.txt$|php/)),
);

const toVisits = (logs: Logs) => pipe(
  logs,
  RA.filter((log) => log.http_user_agent.length > 0),
  RA.filter((log) => !log.http_user_agent.match(/bot|spider|crawler|ubermetrics|dataminr|ltx71|cloud mapping|python-requests|twingly|dark|expanse/i)),
  RA.filter((log) => !log.request.match(/^HEAD /)),
  RA.filter((log) => !log.request.match(/^GET \/static/)),
  RA.filter((log) => !log.request.match(/^GET \/favicon.ico/)),
  RA.map(({
    http_user_agent, request, remote_addr, time_local,
  }) => ({
    visitorId: crypto.createHash('md5').update(`${remote_addr}${http_user_agent}`).digest('hex'),
    time_local,
    request: request.replace(/ HTTP[^ ]+$/, ''),
  })),
  RA.reduce(new Map(), collectPageViewsForVisitor),
  RM.filter(isNotCrawler),
);

const earlierDate = (accum: Date, logEntry: LogEntry) => (
  accum < logEntry.time_local ? accum : logEntry.time_local
);

const laterDate = (accum: Date, logEntry: LogEntry) => (
  accum < logEntry.time_local ? logEntry.time_local : accum
);

const toReport = (logs: Logs) => ({
  logEntriesCount: logs.length,
  logStartTime: RA.reduce(new Date('2970-01-01'), earlierDate)(logs),
  logEndTime: RA.reduce(new Date('1970-01-01'), laterDate)(logs),
  visitors: pipe(
    logs,
    toVisits,
    RM.toReadonlyArray(S.Ord),
  ),
});

const parseFile = flow(
  Json.parse,
  E.chainW(logsFromJson.decode),
  E.map(toReport),
  E.map((report) => JSON.stringify(report, null, 2)),
  // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
  E.getOrElse((e) => { process.stderr.write(`${e}\n`); return ''; }),
);

void (async (): Promise<string> => pipe(
  './reports/2021-09-03.log',
  TE.taskify(fs.readFile),
  TE.map((buffer) => buffer.toString()),
  TE.map(parseFile),
  TE.getOrElse((e) => { process.stderr.write(`${e.toString()}\n`); return T.of(''); }),
  T.map((report) => { process.stdout.write(report); return report; }),
)())();
