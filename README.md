# Streaming tweets about central banks

Tweets about central banks are streamed into an ElasticSearch node on google cloud compute engine using tweepy. 

The `twitter` index has a mapping that designates any field ending in 'created_at' a datetime object.

## Keywords

1. NZ and AUS reserve banks:

  * 'reserve bank', 'glenn stevens', 'graeme wheeler', 'philip lowe', 'phillip lowe'

2. Bank of Canada:

  * 'bank of canada', 'poloz', 'boc rate', 'boc inflation', 'boc monetary', 'boc financial'

3. European Central Bank:

  * 'ecb', 'draghi', 'european central bank'

4. Bank of England:

  * 'bank of england', 'mark carney', 'boe rate', 'boe inflation', 'boe monetary', 'boe financial'

5. Federal Reserve:

  * 'fed', 'federal reserve', 'FOMC', 'yellen', 'powell'

6. Bank of Japan:

  * 'bank of japan', 'kuroda', 'boj rate', 'boj inflation', 'boj monetary', 'boj financial'
