import * as O from 'fp-ts/Option';
import { htmlEscape } from 'escape-goat';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { ViewModel } from './view-model';
import { inputFieldNames } from '../../standards';

const renderErrorSummary = O.match(
  () => '',
  () => `
    <div role='alert' class='error-summary'>
      <h3>Something went wrong</h3>
      <p>The article you are trying to annotate is not currently part of the list.</p>
    </div>
  `,
);

export const renderPage = (viewModel: ViewModel): HtmlFragment => toHtmlFragment(`
  <header class="page-header">
    ${renderErrorSummary(viewModel.unrecoverableError)}
    <h1>${viewModel.pageHeading}</h1>
    <p>Add a public comment to share with others what's interesting or important about this article.</p>
  </header>
  <form class="standard-form" method="POST" action="/annotations/create-annotation">
    <input type="hidden" name="${inputFieldNames.articleId}" value="${viewModel.articleId.value}">
    <input type="hidden" name="${inputFieldNames.listId}" value="${viewModel.listId}">
    <dl>
      <dt>Article</dt>
      <dd>${viewModel.articleTitle}</dd>
      <dt>List</dt>
      <dd>${htmlEscape(viewModel.listName)}</dd>
    </dl>
    <section>
      <label for="annotationContent" class="standard-form__sub_heading">Comment</label>
      <textarea id="annotationContent" name="${inputFieldNames.content}" rows="10" required></textarea>
    </section>
    <button type="submit">Confirm</button>
  </form>
`);
