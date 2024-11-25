const { MongoClient } = require('mongodb');


const agg = [
  //Stage 1: Giving every Document a numerical value according to the month to get chronological order.
  {
    '$addFields': {
      'monthOrder': { '$switch': { 'branches': [
            {
              'case': { '$eq': ['$month', 'OCT'] }, 'then': 1
            }, 
            {
              'case': { '$eq': [ '$month', 'NOV' ] }, 'then': 2
            }, 
            {
              'case': { '$eq': [ '$month', 'DEC' ] }, 'then': 3
            }
          ], 'default': 0
        }}}
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
    '$group': { '_id': '$youtubeChannel', 'metricsByMonth': {
        '$push': { 'month': '$month', 'followers': '$followers', 'likesAvg': '$likesAvg', 'viewsAvg': '$viewsAvg' }
      } }
  }, 
  /*
    Stage 4: 
    Now for pair of months we are calculating gain or loss in like views and followers 
    using the formula [ (current - previous) / previous ] 
  */
  {
    '$set': { 'trends': {
        '$map': { 'input': { '$range': [ 1, { '$size': '$metricsByMonth' } ] },
          'as': 'index',
          'in': { 'monthFrom': { '$arrayElemAt': [ '$metricsByMonth.month', { '$subtract': [ '$$index', 1 ] }  ] }, 'monthTo': { '$arrayElemAt': [ '$metricsByMonth.month', '$$index' ]  },
            'followersChange': {
              '$cond': [ { '$eq': [{ '$arrayElemAt': [ '$metricsByMonth.followers', { '$subtract': [  '$$index', 1 ] } ] }, 0 ]
                }, null, 
                { '$divide': [ {
                      '$subtract': [ 
                        { '$arrayElemAt': [ '$metricsByMonth.followers', '$$index' ] }, 
                        { '$arrayElemAt': [ '$metricsByMonth.followers', {  '$subtract': [   '$$index', 1  ]}]}]
                      }, 
                        { '$arrayElemAt': [  '$metricsByMonth.followers', { '$subtract': [   '$$index', 1  ]}]}]}]},
            'likesAvgChange': {
              '$cond': [{ '$eq': [{ '$arrayElemAt': [ '$metricsByMonth.likesAvg', { '$subtract': [ '$$index', 1 ]}]}, 0 ]
            }, null, 
            { '$divide': [
                    { '$subtract': [ 
                      { '$arrayElemAt': [ '$metricsByMonth.likesAvg', '$$index' ]}, 
                      { '$arrayElemAt': [ '$metricsByMonth.likesAvg', { '$subtract': [ '$$index', 1 ]}]}]
                    }, 
                      { '$arrayElemAt': [ '$metricsByMonth.likesAvg', { '$subtract': [ '$$index', 1 ]}]}]}]},
            'viewsAvgChange': {
              '$cond': [ {
                  '$eq': [ { '$arrayElemAt': [ '$metricsByMonth.viewsAvg', { '$subtract': [ '$$index', 1 ]}]}, 0]}, 
                  null, 
                  { '$divide': [ {
                      '$subtract': [
                        { '$arrayElemAt': [ '$metricsByMonth.viewsAvg', '$$index']},
                        { '$arrayElemAt': [ '$metricsByMonth.viewsAvg', { '$subtract': [ '$$index', 1 ]}]}]
                      }, 
                        { '$arrayElemAt': [ '$metricsByMonth.viewsAvg', { '$subtract': [ '$$index', 1 ]}]}]}]}}}}}
}, 
/* 
  Stage 5: outputing the desired values 
  Youtube Channel Name and the trend array  
*/
{
    '$project': {
      'youtubeChannel': '$_id',
      'trends': {
        '$filter': { 'input': '$trends', 
              'as': 'trend',
              'cond': { '$ne': [ '$$trend', null ]
          }}}}
},
/*
  Stage 6: storing this data in side a new collection for further analysis
*/
{
    '$merge': {
      'into': 'channelTrendsCollection',
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