var async = require('async');
var awsHelpers = require('aws-helpers');
var PriceHistoryRecord = require('price-history-record');
var SageMakerRecord = require('sagemaker-record');

exports.handler = (event, context, callback) => {
    var awsRegion = process.env.AWS_REGION;
    var outputBucketName = process.env.OutputBucketName;
    var outputPrefix = process.env.OutputPrefix;
	console.log('Received event:', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
        var dataString = new Buffer(record.kinesis.data, 'base64').toString('utf8');
        var priceHistoryRecord = JSON.parse(dataString);
        var key = `${outputPrefix}/${priceHistoryRecord.availabilityZone}/${priceHistoryRecord.instanceType}.json`;
        transformData(awsRegion, outputBucketName, key, priceHistoryRecord, function(err, data){
            if(err){
                context.fail("Failed to transform data: " + err);
            }else{
                var serializedTransformedData = JSON.stringify(data);
                awsHelpers.sendDataToS3(awsRegion, outputBucketName, key, serializedTransformedData,function(err){
                    if(err) {
                        context.fail("Failed to persist data: " + err);
                    }
                    else {
                        context.succeed("Successfully persisted data: " + serializedTransformedData);
                    }
                }); 
            }            
        });
    },function(err){
        if(err) {
            context.fail("Failed to process events" + err);
        }
        else {
            context.succeed("Successfully processed " + event.Records.length + " records.");
        }
    });
};

function transformData(awsRegion, bucketName, key, priceHistoryRecord, callback)
{
    console.log('Transforming price history record: %s', JSON.stringify(priceHistoryRecord));
    awsHelpers.readS3Object(awsRegion, bucketName, key, function(err, data){
        if(err){
            if(err.code == "NoSuchKey"){
                console.log('Creating new SageMaker record with timestamp %s', priceHistoryRecord.timestamp);
                var priceHistoryRecordDate = new Date(Date.parse(priceHistoryRecord.timestamp));
                var sageMakerRecord = new SageMakerRecord(priceHistoryRecordDate.toISOString(), [priceHistoryRecord.spotPrice], priceHistoryRecord.category);
                callback(null, sageMakerRecord);
            }else{
                callback(err);
            }  
        }else{
            console.log('Updating existing SageMaker record.');
            var sageMakerRecord = JSON.parse(data);
            sageMakerRecord.cat = priceHistoryRecord.category;
            updateTarget(sageMakerRecord, priceHistoryRecord);
            callback(null, sageMakerRecord);
        }      
    })
};

function getHoursBetween(date1, date2)
{
    var hoursBetween = Math.abs(date1.getTime() - date2.getTime()) / 36e5;
    return hoursBetween;
}

function updateTarget(sageMakerRecord, priceHistoryRecord)
{
    console.log('Adding price history with timestamp %s to SageMaker record starting at %s with %d existing target entries', priceHistoryRecord.timestamp, sageMakerRecord.start, sageMakerRecord.target.length);
    var sageMakerStart = new Date(Date.parse(sageMakerRecord.start));
    var priceHistoryRecordStart = new Date(Date.parse(priceHistoryRecord.timestamp));
    var hoursBetween = getHoursBetween(sageMakerStart, priceHistoryRecordStart);
    console.log('There are %d hours between current SageMaker start and record timestamp', hoursBetween);
    if (sageMakerStart.getTime() > priceHistoryRecordStart.getTime()) {
        console.log('New record timestamp is before SageMaker start, updating SageMaker start and appending price to beginning of existing list.');
        sageMakerRecord.start = priceHistoryRecordStart.toISOString();
        // If there is a gap between the beginning of the current list and the new value, fill with nulls
        var emptyPrices = Array(hoursBetween - 1).fill(null);
        console.log('Padding start of list with %d empty prices', emptyPrices.length);
        var newTargets = [priceHistoryRecord.spotPrice].concat(emptyPrices);
        sageMakerRecord.target = newTargets.concat(sageMakerRecord.target);
    }
    else{
        // If new entry belongs at or past end of list, add any necessary empty entries and append the new entry
        if(hoursBetween - sageMakerRecord.target.length >= 0){
            // If there is a gap between the end of the current list and the new value, fill with nulls
            var emptyPrices = Array(sageMakerRecord.target.length).fill(null);
            console.log('Padding end of list with %d empty prices and adding to end of list', emptyPrices.length);
            sageMakerRecord.target.concat(emptyPrices);
            sageMakerRecord.target.push(priceHistoryRecord.spotPrice)
        }
        else
        {
            // If new entry belongs inside existing list, splice it in
            console.log('Replacing %s with value %s inside existing list', sageMakerRecord.target[hoursBetween], priceHistoryRecord.spotPrice);
            sageMakerRecord.target.splice(hoursBetween, 1, priceHistoryRecord.spotPrice);
        }  
    }
    
    console.log('SageMaker record target now contains %d target entries', sageMakerRecord.target.length);
}