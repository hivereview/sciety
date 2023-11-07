import * as DE from '../../types/data-error';
import { toHtmlFragment } from '../../types/html-fragment';
import { ErrorPageBodyViewModel } from '../../types/render-page-error';

export const toErrorPage = (error: DE.DataError): ErrorPageBodyViewModel => ({
  type: error,
  message: toHtmlFragment(`
    We can't retrieve the information we need to be able to let you create an annotation right now.
    We are sorry for the inconvenience. <a href="https://form.jotform.com/Sciety/error">Report this error to us.</a>
  `),
});
