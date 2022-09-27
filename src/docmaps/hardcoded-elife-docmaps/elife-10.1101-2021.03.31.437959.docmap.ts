import { Docmap } from '../docmap/docmap-type';

export const hardcodedElifeArticle: Docmap = {
  '@context': 'https://w3id.org/docmaps/context.jsonld',
  id: 'https://sciety.org/docmaps/v1/articles/10.1101/2021.03.31.437959/elife.docmap.json',
  type: 'docmap',
  created: '2022-06-14T13:30:54.584Z',
  updated: '2022-07-18T14:30:34.936Z',
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
          doi: '10.1101/2021.03.31.437959',
          url: 'https://doi.org/10.1101/2021.03.31.437959',
        },
      ],
      actions: [
        {
          participants: [
            {
              actor: {
                name: 'anonymous',
                type: 'person',
              },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-06-14T13:28:08.441Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/1F_FtOvlEeyG99OhW4PefQ',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.03.31.437959#hypothesis:1F_FtOvlEeyG99OhW4PefQ',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.03.31.437959/review-three.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: {
                name: 'anonymous',
                type: 'person',
              },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-06-14T13:28:09.213Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/1NN1ruvlEeykz09yoKTuKg',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.03.31.437959#hypothesis:1NN1ruvlEeykz09yoKTuKg',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.03.31.437959/review-two.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: {
                name: 'anonymous',
                type: 'person',
              },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'review-article',
              published: '2022-06-14T13:28:09.942Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/1UK9dOvlEeywYle43r1aAQ',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.03.31.437959#hypothesis:1UK9dOvlEeywYle43r1aAQ',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.03.31.437959/review-one.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: {
                name: 'Patricia J Wittkopp',
                type: 'person',
                _relatesToOrganization: 'University of Michigan, United States',
              },
              role: 'senior-editor',
            },
            {
              actor: {
                name: 'Luis Barreiro',
                type: 'person',
                _relatesToOrganization: 'University of Chicago, United States',
              },
              role: 'editor',
            },
            {
              actor: {
                name: 'Kirk E Lohmueller',
                type: 'person',
                _relatesToOrganization: 'University of California, Los Angeles, United States',
              },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'evaluation-summary',
              published: '2022-06-14T13:28:10.938Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/1dqSyuvlEeyWFs8c2PkP5Q',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.03.31.437959#hypothesis:1dqSyuvlEeyWFs8c2PkP5Q',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.03.31.437959/evaluation-summary.html',
                },
              ],
            },
          ],
        },
        {
          participants: [
            {
              actor: {
                name: 'anonymous',
                type: 'person',
              },
              role: 'peer-reviewer',
            },
          ],
          outputs: [
            {
              type: 'reply',
              published: '2022-07-18T14:29:01.180Z',
              content: [
                {
                  type: 'web-page',
                  url: 'https://hypothes.is/a/99LwwgalEe248ycpDrRwVg',
                },
                {
                  type: 'web-page',
                  url: 'https://sciety.org/articles/activity/10.1101/2021.03.31.437959#hypothesis:99LwwgalEe248ycpDrRwVg',
                },
                {
                  type: 'web-content',
                  url: 'https://sciety.org/static/docmaps/elife-10.1101-2021.03.31.437959/reply.html',
                },
              ],
            },
          ],
        },
      ],
    },
  },
};
