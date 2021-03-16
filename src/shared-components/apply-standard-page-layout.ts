import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { User } from '../types/user';

let googleTagManager = '';
let googleTagManagerNoScript = '';
if (process.env.GOOGLE_TAG_MANAGER_ID) {
  googleTagManager = `
  <script>
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}

    gtag('consent', 'default', {
      'ad_storage': 'denied',
      'analytics_storage': 'denied'
    });
    (function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
    new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
    j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
    'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
    })(window,document,'script','dataLayer','${process.env.GOOGLE_TAG_MANAGER_ID}');
  </script>
  <script>
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());

    gtag('config', '${process.env.GOOGLE_TAG_MANAGER_ID}');
  </script>
`;

  googleTagManagerNoScript = toHtmlFragment(`
<!-- Google Tag Manager (noscript) -->
<noscript><iframe src="https://www.googletagmanager.com/ns.html?id=${process.env.GOOGLE_TAG_MANAGER_ID}"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
<!-- End Google Tag Manager (noscript) -->`);
}

const fathom = process.env.FATHOM_SITE_ID ? `
<script src="https://cdn.usefathom.com/script.js" data-site="${process.env.FATHOM_SITE_ID}" defer></script>
` : '';

const loggedInMenuItems = (user: User) => toHtmlFragment(`
  <li class="site-header__nav_list_item">
    <a href="/users/${user.id}" class="site-header__nav_list_link">My profile</a>
  </li>

  <li class="site-header__nav_list_item">
    <a href="/log-out" class="site-header__nav_list_link">Log out</a>
  </li>
`);

const loggedOutMenuItems = () => toHtmlFragment(`
  <li class="site-header__nav_list_item">
    <a href="/log-in" class="site-header__nav_list_link">Log in</a>
  </li>
`);

const isSecure = process.env.APP_ORIGIN !== undefined && process.env.APP_ORIGIN.startsWith('https:');

// TODO: return a more specific type e.g. HtmlDocument
export const applyStandardPageLayout = (user: O.Option<User>) => (page: Page): string => `<!doctype html>
<html lang="en" prefix="og: http://ogp.me/ns#">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>
    ${htmlEscape(page.title)}
  </title>
  <link rel="stylesheet" href="/static/style.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/cookieconsent/3.1.1/cookieconsent.min.css">
  <meta name="twitter:card" content="summary">
  <meta name="twitter:site" content="@scietyHQ">
  <meta property="og:site_name" content="Sciety">
  <meta property="og:title" content="${htmlEscape(page.openGraph ? page.openGraph.title : 'Sciety')}">
  <meta property="og:description" content="${htmlEscape(page.openGraph ? page.openGraph.description : 'Where research is evaluated and curated by the communities you trust')}">
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
  ${fathom}
</head>
<body>
  ${googleTagManagerNoScript}
  <header class="site-header">
    <div class="site-header__wrapper">
      <a href="/" class="site-header__logo_link">
        <img src="/static/images/sciety-logo-full-colour.svg" alt="Sciety" class="site-header__logo">
      </a>

      <nav class="site-header__nav">

        <ul class="site-header__nav_list" role="list">

          <li class="site-header__nav_list_item">
            <a href="/" class="site-header__nav_list_link">Home</a>
          </li>

          <li class="site-header__nav_list_item">
            <a href="/about" class="site-header__nav_list_link">About</a>
          </li>

          ${O.fold(loggedOutMenuItems, loggedInMenuItems)(user)}

          <li class="site-header__nav_list_item site-header__nav_list_item--search">
            <a href="/search" class="site-header__nav_list_link">
              <img src="/static/images/search-icon.svg" alt="Search" class="site-header__nav_list__search_icon">
            </a>
          </li>

          <li class="site-header__nav_list_item feedback-button">
            <a href="https://eepurl.com/g7qqcv" class="button">Give us feedback</a>
          </li>

        </ul>

      </nav>
    </div>
  </header>

  <main>
    ${page.content}
  </main>

  <footer class="site-footer">
    <small class="site-footer__small_print">
      © 2021 eLife Sciences Publications Ltd.
      <a class="site-footer__link" href="/legal">Legal information</a>
    </small>
  </footer>

  <script src="/static/behaviour.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/cookieconsent/3.1.1/cookieconsent.min.js"></script>
  ${googleTagManager}
  <script>
    function onConsent() {
        if (!this.hasConsented()) {
          return;
        }
        ${process.env.GOOGLE_TAG_MANAGER_ID ? `
          gtag('consent', 'update', {
            'ad_storage': 'denied',
            'analytics_storage': 'granted'
          });
        ` : ''}
    }

    window.cookieconsent.hasTransition = false;
    window.cookieconsent.initialise({
      content: {
        message: 'This site uses cookies to deliver its services and analyse traffic. By using this site, you agree to its use of cookies.',
        href: '/privacy',
        target: '_self'
      },
      onInitialise: onConsent,
      onStatusChange: onConsent,
      palette: {
        popup: {
          background: 'rgb(0, 0, 0, 0.8)',
        }
      },
      cookie: {
        secure: ${isSecure ? 'true' : 'false'}
      },
    });
  </script>
</body>
</html>
`;
