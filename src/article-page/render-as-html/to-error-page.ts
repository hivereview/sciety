import * as DE from '../../types/data-error';
import { toHtmlFragment } from '../../types/html-fragment';
import { RenderPageError } from '../../types/render-page-error';

export const toErrorPage = (error: DE.DataError): RenderPageError => ({
  type: error,
  message: toHtmlFragment(`
    The title and authors for this article are not available from our external data provider.
    We will be able to show you this page once the data becomes available.
    We are sorry for the inconvenience. <a href="https://go.sciety.org/rprterrorpg">Report this error to us.</a>
  `),
});
