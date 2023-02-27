import { pipe } from 'fp-ts/function';
import { renderTabs } from '../../../../shared-components/tabs';
import { HtmlFragment } from '../../../../types/html-fragment';
import { ViewModel } from '../view-model';
import { tabList } from './tab-list';
import { renderListOfListCardsWithFallback } from './render-list-of-list-cards-with-fallback';

const tabProps = (viewmodel: ViewModel) => ({
  tabList: tabList(viewmodel),
  activeTabIndex: 0,
});

export const renderMainContent = (viewmodel: ViewModel): HtmlFragment => pipe(
  renderListOfListCardsWithFallback(viewmodel.activeTab.lists),
  renderTabs(tabProps(viewmodel)),
);
