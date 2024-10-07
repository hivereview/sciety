import { Document, Element } from '@xmldom/xmldom';

export const getElement = (ancestor: Document | Element, qualifiedName: string): Element | null => (
  ancestor.getElementsByTagName(qualifiedName).item(0)
);
