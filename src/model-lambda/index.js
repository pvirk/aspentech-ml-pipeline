var async = require('async');
var awsHelpers = require('aws-helpers');
var crypto = require('crypto');
var PriceHistoryBatch = require('price-history-batch');
var PriceHistoryRecord = require('price-history-record');

exports.handler = (event, context, callback) => {
    var awsRegion = process.env.AWS_REGION;
    var firehoseStream = process.env.FirehoseStream;
    var outputStream = process.env.OutputStream;
    console.log('Received event:', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
        var serializedData = new Buffer(record.kinesis.data, 'base64').toString('utf8');
        var priceHistoryBatch = JSON.parse(serializedData);
        console.log("Received %d ingest records", priceHistoryBatch.records.length);
        var updatedData = modelData(priceHistoryBatch);

        awsHelpers.sendDataToFirehose(awsRegion, firehoseStream, JSON.stringify(updatedData), function(err){
            async.each(updatedData, function(data, callback){
                var partitionKey = buildPartitionKey(data);
                awsHelpers.sendDataToKinesis(awsRegion, outputStream, partitionKey, JSON.stringify(data), callback);
            },function(err){
                callback(err);
            });
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

function modelData(priceHistoryBatch)
{
    var records = [];
    for(var i = 0; i < priceHistoryBatch.records.length; i++){
        var historyEntry = priceHistoryBatch.records[i];
        var record = new PriceHistoryRecord(historyEntry.AvailabilityZone,
                                historyEntry.InstanceType,
                                historyEntry.SpotPrice,
                                historyEntry.Timestamp,
                                null);
        console.log("Adding price history record: " + JSON.stringify(record));
        records.push(record);
    }
    return records;
}

function buildPartitionKey(entry){
    var keyValues = `${entry.availabilityZone}|${entry.instanceType}`;
    return crypto.createHash('md5').update(keyValues).digest('hex');
}