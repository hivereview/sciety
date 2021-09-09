import * as crypto from 'crypto';
import * as D from 'fp-ts/Date';
import * as Ord from 'fp-ts/Ord';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RM from 'fp-ts/ReadonlyMap';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import * as LF from './log-file';
import { PageView } from './page-view';
import * as Sess from './session';

type ObfuscatedPageView = PageView & {
  visitorId: string,
};

type Visits = Map<string, ReadonlyArray<PageView>>;

const collectPageViewsForVisitor = (accum: Visits, pageView: ObfuscatedPageView): Visits => {
  const pvs = accum.get(pageView.visitorId) ?? [];
  return accum.set(pageView.visitorId, pvs.concat({
    time_local: pageView.time_local,
    request: pageView.request,
  }));
};

const isNotCrawler = (pageViews: ReadonlyArray<PageView>) => pipe(
  pageViews,
  RA.every((v) => !v.request.match(/\/robots.txt$|php|\.env/)),
);

const byDate: Ord.Ord<PageView> = pipe(
  D.Ord,
  Ord.contramap((event) => event.time_local),
);

const toVisitors = (logs: LF.Logs) => pipe(
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
  RM.map(RA.sort(byDate)),
);

const toSessions = (visitors: ReadonlyMap<string, ReadonlyArray<PageView>>): ReadonlyArray<Sess.Session> => pipe(
  visitors,
  RM.toReadonlyArray(S.Ord),
  RA.map(([visitorId, pageViews]) => ({
    visitorId,
    pageViews,
  })),
  RA.chain(Sess.split),
);

const countBreadcrumbInitiation = (sessions: ReadonlyArray<Sess.Session>) => pipe(
  sessions,
  RA.filter((session) => pipe(
    session.pageViews,
    RA.some((pageView) => pageView.request.startsWith('POST ')),
  )),
  RA.size,
);

const toVisitorsReport = (logFile: LF.LogFile) => pipe(
  logFile.logEntries,
  toVisitors,
  (visitors) => ({
    logEntriesCount: logFile.logEntriesCount,
    logStartTime: logFile.logStartTime,
    logEndTime: logFile.logEndTime,
    visitorsCount: pipe(visitors, RM.size),
    sessions: pipe(visitors, toSessions),
  }),
  (partial) => ({
    ...partial,
    countOfSessions: partial.sessions.length,
    countOfSessionsInitiatingBreadcrumbs: countBreadcrumbInitiation(partial.sessions),
  }),
  (report) => JSON.stringify(report, null, 2),
);

void (async (): Promise<string> => pipe(
  './reports/ingress-logs.jsonl',
  LF.read,
  TE.map(toVisitorsReport),
  TE.match(
    (e) => { process.stderr.write(`${e}\n`); return ''; },
    (report) => { process.stdout.write(report); return report; },
  ),
)())();
