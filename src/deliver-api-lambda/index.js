var awsHelpers = require('aws-helpers');

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    var startDate = new Date(Date.parse(event.startDate));
    var endDate = new Date(Date.parse(event.endDate));
    var az = event.availabilityZone;
    var instanceType = event.instanceType;
    console.log('Searching for data between %s and %s', event.startDate, event.endDate);
    readData(startDate, endDate, az, instanceType, callback);
};


function readData(startDate, endDate, az, instanceType, callback)
{
    var awsRegion = process.env.AWS_REGION;
    var bucketName = process.env.OutputBucketName;
    var key = [process.env.ObjectPrefix, az, instanceType].join('/') + ".json";
    console.log("Reading transform file s3://%s/%s", bucketName, key);
    awsHelpers.readS3Object(awsRegion, bucketName, key, function(err, object) {
        if(err) { 
            console.log(err, err.stack);
            callback(err); 
        } else {
            var objectContent = JSON.parse(object);
            var objectStart = new Date(Date.parse(objectContent.start));
            var startIndex = numHoursBetween(objectStart, startDate);
            var length = numHoursBetween(startDate, endDate);
            var totalLength = objectContent.target.length;
            console.log("History Start: %s; Start index: %d; length: %d; totalLength: %d", objectStart, startIndex, length, totalLength);
            callback(null, objectContent.target.slice(startIndex, startIndex+length));
        }
    });
}

function numHoursBetween(startDate, stopDate) {
    return stopDate.getTime()/3600000 - startDate.getTime()/3600000;
}
