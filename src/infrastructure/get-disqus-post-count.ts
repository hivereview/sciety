import createLogger from '../logger';
import GetCommentCountError from '../types/get-comment-count-error';
import { Json, JsonCompatible } from '../types/json';

export type GetJson = (uri: string) => Promise<Json>;

type DisqusData = JsonCompatible<{
  response: Array<{
    posts: number;
  }>;
}>;

type GetDisqusPostCount = (uri: string) => Promise<number>;

export default (getJson: GetJson): GetDisqusPostCount => {
  const log = createLogger('infrastructure:fetch-disqus-post-count');
  return async (uri) => {
    log(`Fetching Disqus threads list for ${uri}`);
    try {
      const disqusData = await getJson(`https://disqus.com/api/3.0/threads/list.json?api_key=${process.env.DISQUS_API_KEY}&forum=biorxivstage&thread=link:${uri}`) as DisqusData;
      log(`Disqus response: ${JSON.stringify(disqusData)}`);

      return disqusData.response[0].posts;
    } catch (e) {
      const message = `Disqus API error: ${e.message}`;
      log(message);

      throw new GetCommentCountError(message);
    }
  };
};
