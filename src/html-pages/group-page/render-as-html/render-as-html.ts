import { Page } from '../../../types/page';
import { ViewModel } from '../view-model';
import { renderPage } from './render-page';

// ts-unused-exports:disable-next-line
export const renderAsHtml = (viewmodel: ViewModel): Page => ({
  title: viewmodel.group.name,
  content: renderPage(viewmodel),
});
