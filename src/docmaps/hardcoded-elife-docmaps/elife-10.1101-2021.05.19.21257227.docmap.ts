import { Docmap } from '../docmap/docmap-type';

export const hardcodedElifeArticle: Docmap = {
  '@context': 'https://w3id.org/docmaps/context.jsonld',
  id: 'https://sciety.org/docmaps/v1/evaluations-by/elife/10.1101/2021.06.02.446694.docmap.json',
  type: 'docmap',
  created: '2022-02-15T09:50:29.018Z',
  updated: '2022-02-15T11:30:27.034Z',
  publisher: {
    id: 'https://elifesciences.org/',
    name: 'eLife',
    logo: 'https://sciety.org/static/groups/elife--b560187e-f2fb-4ff9-a861-a204f3fc0fb0.png',
    homepage: 'https://elifesciences.org/',
    account: {
      id: 'https://sciety.org/groups/elife',
      service: 'https://sciety.org',
    },
  },
  'first-step': '_:b0',
  steps: {
    '_:b0': {
      assertions: [],
      inputs: [
        {
          doi: '10.1101/2021.06.02.446694',
          url: 'https://doi.org/10.1101/2021.06.02.446694',
        },
      ],
      actions: [
        {
          participants: [
            {
              actor: { name: 'anonymous', type: 'person' },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-02-15T09:43:12.593Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/sQ7jVo5DEeyQwX8SmvZEzw',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.06.02.446694#hypothesis:sQ7jVo5DEeyQwX8SmvZEzw',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.06.02.446694/review-three.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: { name: 'anonymous', type: 'person' },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-02-15T09:43:13.592Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/saaeso5DEeyNd5_qxlJjXQ',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.06.02.446694#hypothesis:saaeso5DEeyNd5_qxlJjXQ',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.06.02.446694/review-two.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: { name: 'anonymous', type: 'person' },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-02-15T09:43:14.350Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/shmDUI5DEey0T6t05fjycg',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.06.02.446694#hypothesis:shmDUI5DEey0T6t05fjycg',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.06.02.446694/review-one.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: { name: 'Ronald L Calabrese', type: 'person', _relatesToOrganization: 'Emory University, United States' },
              role: 'senior-editor',
            },
            {
              actor: { name: 'Noah J Cowan', type: 'person', _relatesToOrganization: 'Johns Hopkins University, United States' },
              role: 'editor',
            },
          ],
          outputs: [
            {
              type: 'evaluation-summary',
              published: '2022-02-15T09:43:15.348Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/srHqyI5DEeyY91tQ-MUVKA',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.06.02.446694#hypothesis:srHqyI5DEeyY91tQ-MUVKA',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.06.02.446694/evaluation-summary.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: { name: 'anonymous', type: 'person' },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'reply',
              published: '2022-02-15T11:24:05.730Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/ySfx9I5REeyOiqtIYslcxA',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.06.02.446694#hypothesis:ySfx9I5REeyOiqtIYslcxA',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.06.02.446694/reply.html',
                },
              ],
            },
          ],
        },
      ],
    },
  },
};
