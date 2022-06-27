import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { constant } from 'fp-ts/function';
import { siteMenuItems } from './site-menu';
import { googleTagManagerNoScript } from '../shared-components/analytics';
import { head } from '../shared-components/head';
import { utilityBar } from '../shared-components/utility-bar';
import { User } from '../types/user';

// TODO: return a more specific type e.g. HtmlDocument
export const menuPageLayout = (user: O.Option<User>, referer: O.Option<string>): string => `<!doctype html>
<html lang="en" prefix="og: http://ogp.me/ns#">
  ${head(user, { title: 'Menu' })}
<body>
  ${googleTagManagerNoScript()}

<div class="menu-page-container">

  ${htmlEscape`<a href="${O.getOrElse(constant('/'))(referer)}" class="menu-page__close_nav"><img src="/static/images/close-icon.svg" alt=""></a>`}

  <main class="menu-page-main-content">
    <nav class="navigation-menu">
      <h1 class="navigation-menu__title">Menu</h1>
      ${siteMenuItems(user)}
    </nav>
  </main>

  <header class="menu-page-header">
    <a href="/search" class="site-header__search_link">
      <img src="/static/images/search-icon.svg" alt="" class="site-header__search_icon"><span class="site-header__search_label">Search</span>
    </a>
    ${utilityBar(user)}
  </header>

</div>

  <script src="/static/behaviour.js"></script>

</body>
</html>
`;
