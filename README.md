# Streaming tweets about central banks

Tweets about central banks are streamed into an ElasticSearch node on google cloud compute engine using tweepy. 

The `twitter` index has a mapping that designates any field ending in 'created_at' a datetime object.

## Keywords

1. __NZ and AUS reserve banks__: 'reserve bank', 'glenn stevens', 'graeme wheeler', 'philip lowe', 'phillip lowe'

2. __Bank of Canada__: 'bank of canada', 'poloz', 'boc rate', 'boc inflation', 'boc monetary', 'boc financial'

3. __European Central Bank__: 'ecb', 'draghi', 'european central bank'

4. __Bank of England__: 'bank of england', 'mark carney', 'boe rate', 'boe inflation', 'boe monetary', 'boe financial'

5. __Federal Reserve__: 'fed', 'fomc', 'yellen', 'powell'

6. __Bank of Japan__: 'bank of japan', 'kuroda', 'boj rate', 'boj inflation', 'boj monetary', 'boj financial'
