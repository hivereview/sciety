export type Participant = PeerReviewer | SeniorEditor | Editor;

type PeerReviewer = {
  actor: {
    name: string,
    type: 'person',
  },
  role: 'peer-reviewer',
};

type SeniorEditor = {
  actor: {
    name: string,
    type: 'person',
    _relatesToOrganization: string,
  },
  role: 'senior-editor',
};

type Editor = {
  actor: {
    name: string,
    type: 'person',
    _relatesToOrganization: string,
  },
  role: 'editor',
};

type Output = {
  type: 'review-article' | 'evaluation-summary' | 'reply',
  published: string,
  content: ReadonlyArray<unknown>,
};

type Action = {
  participants: ReadonlyArray<Participant>,
  outputs: ReadonlyArray<Output>,
};

type Input = {
  doi: string,
  url: string,
  published?: string,
};

type Step = {
  assertions: [],
  inputs: ReadonlyArray<Input>,
  actions: ReadonlyArray<Action>,
};

type Publisher = {
  id: string,
  name: string,
  logo: string,
  homepage: string,
  account: {
    id: string,
    service: 'https://sciety.org',
  },
};

export type Docmap = {
  '@context': string,
  id: string,
  type: 'docmap',
  created: string,
  updated: string,
  publisher: Publisher,
  'first-step': '_:b0',
  steps: Record<string, Step>,
};
