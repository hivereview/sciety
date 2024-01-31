import { Middleware } from '@koa/router';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { ParameterizedContext } from 'koa';
import { standardPageLayout } from '../shared-components/standard-page-layout';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from './authentication-and-logging-in-of-sciety-users';
import { ConstructPage } from '../html-pages/construct-page';
import { PageLayout } from '../html-pages/page-layout';
import { constructHtmlResponse } from '../html-pages/construct-html-response';
import { sendHtmlResponse } from './send-html-response';
import { detectClientClassification } from './detect-client-classification';
import { ErrorPageBodyViewModel } from '../types/render-page-error';
import { sendRedirect } from './send-redirect';
import { HtmlPage } from '../html-pages/html-page';

const constructAndSendHtmlResponse = (
  adapters: GetLoggedInScietyUserPorts,
  pageLayout: PageLayout,
  context: ParameterizedContext,
) => (input: E.Either<ErrorPageBodyViewModel, HtmlPage>) => pipe(
  input,
  constructHtmlResponse(
    getLoggedInScietyUser(adapters, context),
    pageLayout,
    detectClientClassification(context),
  ),
  sendHtmlResponse(context),
);

export const pageHandler = (
  adapters: GetLoggedInScietyUserPorts,
  handler: ConstructPage,
  pageLayout: PageLayout = standardPageLayout,
): Middleware => async (context, next) => {
  const input = await pipe(
    {
      ...context.params,
      ...context.query,
      ...context.state,
    },
    (partialParams) => pipe(
      getLoggedInScietyUser(adapters, context),
      O.foldW(
        () => ({
          ...partialParams,
          user: undefined,
        }),
        (user) => ({
          ...partialParams,
          user,
        }),
      ),
    ),
    handler,
  )();
  if (E.isLeft(input)) {
    constructAndSendHtmlResponse(adapters, pageLayout, context)(input);
  } else if (input.right.tag === 'redirect-target') {
    sendRedirect(context, input.right);
  } else {
    constructAndSendHtmlResponse(adapters, pageLayout, context)(E.right(input.right));
  }

  await next();
};
