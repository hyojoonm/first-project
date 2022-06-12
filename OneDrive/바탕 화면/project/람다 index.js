const AWS = require('aws-sdk');
const mysql = require('mysql');

const R = new AWS.Rekognition();
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: 3306,
});

exports.handler = async (event, context, lambdaCallback) => {
  try {
    context.callbackWaitsForEmptyEventLoop = false;

    const img = event.Records[0].s3.object.key,
      s3Bucket = event.Records[0].s3.bucket.name;
    
    const result = await R.detectFaces({
      Attributes: ["ALL"],
      Image: {
        S3Object: {
          Bucket: s3Bucket,
          Name: img,
        },
      },
    }).promise();

    const connection = await getConnection();
    for (let i = 0; i < result.FaceDetails.length; i++) {
      const reko = result.FaceDetails[i];
      await runQuery(connection, reko, s3Bucket, img);
    }
    lambdaCallback(null);
  } catch (err) {
    lambdaCallback(err);
  }
};

const getConnection = () => {
  return new Promise((resolve, reject) => {
    pool.getConnection(function(error, connection) {
      if (error) {
        reject(error);
      } else {
        resolve(connection);
      }
    });
  });
};

const runQuery = (conn, reko, bucket, key) => {
  const { Low, High } = reko.AgeRange;
  const gender = reko.Gender.Value;

  return new Promise((resolve, reject) => {
    conn.query('INSERT INTO emotion (lowAge, highAge, gender, emotions, bucket, name) VALUES (?, ?, ?, ?, ?, ?)', [Low, High, gender, JSON.stringify(reko.Emotions), bucket, key], function (error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};