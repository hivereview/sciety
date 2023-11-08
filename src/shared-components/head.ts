import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { fathom, googleTagManager } from './analytics';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { UserId } from '../types/user-id';
import { HtmlPage } from '../html-pages/html-page';

export type ClientClassification = {
  userAgent: string | undefined,
};

export type DynamicHeadViewModel = Omit<HtmlPage, 'content'>;

export const head = (
  userId: O.Option<UserId>,
  dynamicHeadViewModel: DynamicHeadViewModel,
): HtmlFragment => toHtmlFragment(`
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>
    ${htmlEscape(dynamicHeadViewModel.title.startsWith('Sciety') ? dynamicHeadViewModel.title : `${dynamicHeadViewModel.title} | Sciety`)}
  </title>
  ${(dynamicHeadViewModel.description) ? htmlEscape`<meta name="description" content="${dynamicHeadViewModel.description}">` : ''}
  <link rel="stylesheet" href="/static/style.css">
  <meta name="twitter:card" content="summary">
  <meta name="twitter:site" content="@scietyHQ">
  <meta property="og:site_name" content="Sciety">
  <meta property="og:title" content="${htmlEscape(dynamicHeadViewModel.openGraph ? dynamicHeadViewModel.openGraph.title : 'Sciety')}">
  <meta property="og:description" content="${htmlEscape(dynamicHeadViewModel.openGraph ? dynamicHeadViewModel.openGraph.description : 'Let Sciety help you navigate the preprint landscape.')}">
  <meta property="og:image" content="${process.env.APP_ORIGIN ?? ''}/static/images/sciety-twitter-profile.png">
  <link rel="icon" type="image/svg+xml" href="/static/images/favicons/favicon.svg">

  <link rel="apple-touch-icon" sizes="180x180" href="/static/images/favicons/generated/apple-touch-icon.png">
  <link rel="icon" type="image/png" sizes="32x32" href="/static/images/favicons/generated/favicon-32x32.png">
  <link rel="icon" type="image/png" sizes="16x16" href="/static/images/favicons/generated/favicon-16x16.png">
  <link rel="manifest" href="/static/images/favicons/generated/site.webmanifest">
  <link rel="mask-icon" href="/static/images/favicons/generated/safari-pinned-tab.svg" color="#cf4500">
  <link rel="shortcut icon" href="/static/images/favicons/generated/favicon.ico">
  <meta name="msapplication-TileColor" content="#cf4500">
  <meta name="msapplication-config" content="/static/images/favicons/generated/browserconfig.xml">
  <meta name="theme-color" content="#ffffff">

  ${googleTagManager(userId)}
  ${fathom()}
</head>
`);
