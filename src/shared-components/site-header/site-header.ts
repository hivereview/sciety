import * as O from 'fp-ts/Option';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { UserDetails } from '../../types/user-details';
import { utilityBar } from '../utility-bar';

export const siteHeader = (user: O.Option<UserDetails>): HtmlFragment => toHtmlFragment(`<header class="site-header">
  <a href="#mainContent" class="visually-hidden">Skip navigation</a>
  <div class="site-header__white_box_padding"></div>  
  <nav class="site-header__white_box">
    <ul class="site-header__white_box_list">
      <li class="site-header__white_box_list_item--logo">
        <a href="/" class="site-header__logo_link">
          <img src="/static/images/sciety-logo-navigation-link-colour-text.svg " alt="Sciety" class="site-header__logo">
        </a>
      </li>
      <li class="site-header__white_box_list_item--menu">
        <a href="/menu" class="site-header__menu_link">
          <img src="/static/images/menu-icon.svg" alt="" />
        </a>
      </li>
      <li>
        <a href="/search" class="site-header__search_link">
                <svg width="24px" height="24px" viewBox="0 0 24 24" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
          <g id="Designs" stroke="none" stroke-width="1" fill="none" fill-rule="evenodd">
              <g id="results-mobile-broken-down" transform="translate(-367.000000, -169.000000)">
                  <g id="Group" transform="translate(95.000000, 112.000000)">
                      <g id="new/navbar-mobile" transform="translate(0.000000, 36.000000)">
                          <g id="page-tools" transform="translate(168.000000, 12.000000)">
                              <g id="baseline-search-24px" transform="translate(104.000000, 9.000000)">
                                  <path class="site-header__search_icon_path" d="M15.5,14 L14.71,14 L14.43,13.73 C15.41,12.59 16,11.11 16,9.5 C16,5.91 13.09,3 9.5,3 C5.91,3 3,5.91 3,9.5 C3,13.09 5.91,16 9.5,16 C11.11,16 12.59,15.41 13.73,14.43 L14,14.71 L14,15.5 L19,20.49 L20.49,19 L15.5,14 Z M9.5,14 C7.01,14 5,11.99 5,9.5 C5,7.01 7.01,5 9.5,5 C11.99,5 14,7.01 14,9.5 C14,11.99 11.99,14 9.5,14 Z" id="Shape" fill="#34434A" fill-rule="nonzero"/>
                                  <polygon id="Path" points="0 0 24 0 24 24 0 24"/>
                              </g>
                          </g>
                      </g>
                  </g>
              </g>
          </g>
      </svg>
      <span class="site-header__search_label">Search</span>
        </a>
      </li>
    </ul>
  </nav>
  <div class="site-header__grey_box">
    ${utilityBar(user)}
  </div>
  <div></div> 
</header>`);
