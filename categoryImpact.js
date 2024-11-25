const  { MongoClient }  = require('mongodb');


const agg = [
  /*
   Stage 1: 
   grouping based on category and then geting the sum of each cateogry
  */
  {
    '$group': { '_id': '$category', 
      'totalFollowers': 
      {'$sum': '$followers'}, 
      'avgLikes': 
      { '$avg': '$likesAvg'}, 
      'avgViews': 
      {'$avg': '$viewsAvg'}, 
      'avgComments': 
      {'$avg': '$commentsAvg'}, 
      'influencerCount': 
      {'$sum': 1}
    }
  }, 
  /*
    Stage 2: calculate the engamnet 
  */
  {
    '$addFields': { 'engagementRate': {
        '$cond': [
          { '$eq': ['$totalFollowers', 0]}, 0, 
          {
            '$divide': [
              { '$add': [ '$avgLikes', '$avgComments', '$avgViews'] }, '$totalFollowers']
          }]}}
  }, 
  /* 
    Stage 3: decreaing order according to the engament 
  */
  {
    '$sort': {'engagementRate': -1}
  }, 
  /*
    Stage 4: Output of the pipeline
  */
  {
    '$project': {
      'category': '$_id', 
      '_id': 0, 
      'totalFollowers': 1, 
      'avgLikes': 1, 
      'avgViews': 1, 
      'avgComments': 1, 
      'influencerCount': 1, 
      'engagementRate': 1
    }
  },
  /* 
    Stage 5: storing this data in side a new collection for further analysis
*/
  {
    '$merge': {
      'into': 'categoryCollection',
      'whenMatched': 'merge',
      'whenNotMatched': 'insert' 
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