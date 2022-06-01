import * as O from 'fp-ts/Option';
import { utilityBar } from './utility-bar';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { User } from '../types/user';

export const siteHeader = (user: O.Option<User>): HtmlFragment => toHtmlFragment(`<header class="site-header">
    <div class="site-header__inner">
    <a href="/menu" class="site-header__menu_link">
        <img src="/static/images/menu-icon.svg" alt="" />
    </a>

    ${utilityBar(user)}
    </div>
</header>`);
