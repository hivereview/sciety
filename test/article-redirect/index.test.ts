import { PERMANENT_REDIRECT } from 'http-status-codes';
import request, { Response } from 'supertest';
import createServer from '../handlers/server';

const arbitraryDoi = '10.1101/2000.1234';

describe('article redirect route', (): void => {
  let response: Response;

  beforeEach(async () => {
    const { server } = createServer();
    response = await request(server).get(`/articles?doi=${arbitraryDoi}`);
  });

  it('displays search results', async (): Promise<void> => {
    expect(response.text).toStrictEqual(expect.stringContaining('Search results'));
  });
});
