const  { MongoClient }  = require('mongodb');



const agg = [
  //Stage 1: Giving every Document a numerical value according to the month to get chronological order.
  {
    '$addFields': {
      'monthOrder': { '$switch': { 'branches': [
            {
              'case': { '$eq': [ '$month', 'OCT' ] }, 'then': 1
            }, 
            {
              'case': { '$eq': [ '$month', 'NOV' ] },  'then': 2
            }, 
            {
              'case': { '$eq': [ '$month', 'DEC' ] },  'then': 3
            }
          ], 'default': 0 }}}
  }, 
  //Stage 2: Ascending order according to month and youtube Channel
  {
    '$sort': { 'youtubeChannel': 1, 'monthOrder': 1 }
  }, 
  /*
    Stage 3: Now grouping and creating an array having values as 
    month | followers | likesAvg | viewAvg
  */
  {
    '$group': {
      '_id': '$youtubeChannel', 
      'metricsByMonth': {
        '$push': { 'month': '$month',  'followers': '$followers',  'likesAvg': '$likesAvg',  'viewsAvg': '$viewsAvg',  'commentsAvg': '$commentsAvg' }}}
  }, 
  /*
    Stage 4: 
    we first created the follwer change then 
    engamnet by adding (like + comment + views) / followers
    then engament change 
  */
  {
    '$set': {
      'trends': { '$map': {
          'input': { '$range': [1, { '$size': '$metricsByMonth'}]
          }, 
          'as': 'index', 
          'in': { 'monthFrom': { '$arrayElemAt': [ '$metricsByMonth.month', { '$subtract': [ '$$index', 1]} ]
            },  'monthTo': {'$arrayElemAt': ['$metricsByMonth.month', '$$index'] }, 
            'followersChange': {
              '$cond': [
                { '$eq': [{ '$arrayElemAt': [ '$metricsByMonth.followers', { '$subtract': ['$$index', 1]}]}, 0]
                }, null, 
                { '$divide': [
                    { '$subtract': [
                        {
                          '$arrayElemAt': [ '$metricsByMonth.followers', '$$index']}, 
                        {
                          '$arrayElemAt': [ '$metricsByMonth.followers', {'$subtract': ['$$index', 1] }]
                        }]
                    }, 
                    {'$cond': [
                        {
                          '$eq': [{'$arrayElemAt': ['$metricsByMonth.followers', {'$subtract': [  '$$index', 1]}]}, 0]
                        }, 1, 
                        {'$arrayElemAt': ['$metricsByMonth.followers', {'$subtract': ['$$index', 1]}]}
                      ]}]}]}, 
            'engagementRate': {
              '$cond': [
                {'$or': [{'$eq': [{'$arrayElemAt': ['$metricsByMonth.followers', '$$index']}, 0]}, 
                        {'$eq': [{'$arrayElemAt': ['$metricsByMonth.likesAvg', '$$index']}, 0]}]
                }, 0, {
                  '$multiply': [{
                      '$add': 
                      [{'$arrayElemAt': ['$metricsByMonth.likesAvg', '$$index']}, 
                      {'$arrayElemAt': ['$metricsByMonth.viewsAvg', '$$index']}, 
                      {'$arrayElemAt': ['$metricsByMonth.commentsAvg', '$$index']}] }, 
                    {
                      '$divide': [1, {'$arrayElemAt': ['$metricsByMonth.followers', '$$index']}]
                    }]}]}, 
            'engagementRateChange': {
              '$cond': [
                {
                  '$eq': [
                    {'$arrayElemAt': ['$metricsByMonth.likesAvg', {'$subtract': ['$$index', 1]}]}, 0]
                }, null, {
                  '$divide': [
                    {
                      '$subtract': [
                        {'$arrayElemAt': ['$metricsByMonth.likesAvg', '$$index']}, 
                        {'$arrayElemAt': ['$metricsByMonth.likesAvg', {'$subtract': ['$$index', 1 ]}]}]
                    }, {
                      '$cond': [
                        {'$eq': [
                            {'$arrayElemAt': ['$metricsByMonth.likesAvg', {'$subtract': ['$$index', 1]}]}, 0]
                        }, 1, 
                        {'$arrayElemAt': ['$metricsByMonth.likesAvg', {'$subtract': ['$$index', 1]}]}
                      ]}]}]}}}}}
},  
/* 
    Stage 5: storing this data in side a new collection for further analysis
*/
{
  '$merge': {
    'into': 'EngagementTrendsCollection',
    'whenMatched': 'merge', // 'merge' merges the existing documents with the new ones
    'whenNotMatched': 'insert' // 'insert' inserts the new document if not already present
  }
}
];
const connect = async () => {
    const client = await MongoClient.connect(
      'mongodb+srv://vivekshaurya62:DSABD@dsabd.dz7ok.mongodb.net/'
    );
    return client;
  };
  
  (async () => {
    const client = await connect();
    try {
      const coll = client.db('InfluencersData').collection('influencers');
      const cursor = coll.aggregate(agg);
      const result = await cursor.toArray();
      console.log(result);
    } catch (error) {
      console.error('Error:', error);
    } finally {
      await client.close();
    }
  })();