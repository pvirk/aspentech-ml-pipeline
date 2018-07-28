var async = require('async');
var awsHelpers = require('aws-helpers');
var crypto = require('crypto');
var PriceHistoryRecord = require('price-history-record');
var awsConstants = require('aws-constants');

exports.handler = (event, context, callback) => {
    var awsRegion = process.env.AWS_REGION;
    var firehoseStream = process.env.FirehoseStream;
    var outputStream = process.env.OutputStream;
	console.log('Received event: ', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
        var dataString = new Buffer(record.kinesis.data, 'base64').toString('utf8');
        var priceHistoryRecord = JSON.parse(dataString);
        var updatedData = enhanceData(priceHistoryRecord);
        var serializedUpdatedData = JSON.stringify(updatedData);
        console.log('Enhanced data: ', serializedUpdatedData);
        awsHelpers.sendDataToFirehose(awsRegion, firehoseStream, serializedUpdatedData, function(err){
            if(err) {
                context.fail("Failed to send data to firehouse" + err);
            }
            awsHelpers.sendDataToKinesis(awsRegion, outputStream, record.kinesis.partitionKey, serializedUpdatedData, callback);
        });        
    },function(err){
        if(err) {
            context.fail("Failed to process events" + err);
        }
        else {
            context.succeed("Successfully processed " + event.Records.length + " records.");
        }
    });
}

function enhanceData(priceHistoryRecord)
{
    var copy = JSON.parse(JSON.stringify(priceHistoryRecord));
    copy.category = getCategory(priceHistoryRecord.availabilityZone, priceHistoryRecord.instanceType);
    return copy;
}

function getCategory(availabilityZone, instanceType)
{
    var category = -1;
    var categoryKey = `${availabilityZone}/${instanceType}`;
    if (categoryKey in awsConstants.CATEGORIES) {
        var category = awsConstants.CATEGORIES[categoryKey];
        console.log(`Found category ${category} for availabilityZone ${availabilityZone} and instanceType ${instanceType}`);
    }else{
        console.log("Failed to find matching category for instance type %s and availability zone %s", instanceType, availabilityZone);
    }
    return category;
}