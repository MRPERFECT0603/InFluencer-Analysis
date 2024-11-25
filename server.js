const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');

// Step 1: Define the schema
const InfluencerSchema = new mongoose.Schema({
  youtubeChannel: String,
  youtuberName: String,
  category: String,
  followers: Number,
  country: String,
  viewsAvg: Number,
  likesAvg: Number,
  commentsAvg: Number,
  category2: String,
  month: String,
});

// Step 2: Create the model
const Influencer = mongoose.model('Influencer', InfluencerSchema);

// Function to parse "M" and "K" values to numbers
function parseSuffixValue(value) {
    if (typeof value !== 'string') return 0;
    value = value.trim();
    if (value.endsWith('M')) {
      return parseFloat(value.replace('M', '')) * 1e6;
    } else if (value.endsWith('K')) {
      return parseFloat(value.replace('K', '')) * 1e3;
    } else {
      return parseFloat(value) || 0; // Handle raw numbers or invalid cases
    }
  }
  
  mongoose
    .connect('mongodb+srv://vivekshaurya62:DSABD@dsabd.dz7ok.mongodb.net/InfluencersData')
    .then(() => {
      console.log('Connected to MongoDB');
  
      fs.createReadStream('influencersDEC.csv')
        .pipe(csv())
        .on('data', async (row) => {
          const influencer = new Influencer({
            youtubeChannel: row['YoutubeChannel'],
            youtuberName: row['youtuberName'],
            category: row['Category'],
            followers: parseSuffixValue(row['Followers']),
            country: row['Country'] || 'Unknown',
            viewsAvg: parseSuffixValue(row['Views (Avg.)']),
            likesAvg: parseSuffixValue(row['Likes (Avg.)']),
            commentsAvg: parseSuffixValue(row['Comments (Avg.)']),
            category2: row['Category-2'] || null,
            month: 'DEC',
          });
          try {
            await influencer.save();
            console.log(`Inserted: ${row['YoutubeChannel']}`);
          } catch (error) {
            console.error('Error inserting data:', error);
          }
        })
        .on('end', () => {
          console.log('CSV file successfully processed');
        //   mongoose.disconnect(); // Close the MongoDB connection
        });
    })
    .catch((err) => {
      console.error('Connection error:', err);
    });