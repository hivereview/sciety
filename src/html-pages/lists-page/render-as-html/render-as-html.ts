import { ViewModel } from '../view-model.js';
import { HtmlPage, toHtmlPage } from '../../html-page.js';
import { renderPage } from './render-page.js';

export const renderAsHtml = (viewModel: ViewModel): HtmlPage => toHtmlPage({
  title: 'Lists',
  content: renderPage(viewModel),
});
