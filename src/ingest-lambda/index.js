var AWS = require('aws-sdk');
var async = require('async');
var awsHelpers = require('aws-helpers');
var crypto = require('crypto');
var PriceHistoryBatch = require('price-history-batch');
var awsConstants = require('aws-constants');

exports.handler = (event, context, callback) => {
	console.log('Ingest handler received event:', event);
	if(event.hasOwnProperty('Records')){
		console.log('Triggered by Kinesis');
		async.each(event.Records, function(record, callback){
			var serializedData = new Buffer(record.kinesis.data, 'base64').toString('utf8');
			var request = JSON.parse(serializedData);
			console.log('Request: ' + serializedData);
			var startTime = new Date(Date.parse(request.startDate));
			var endTime = new Date(Date.parse(request.endDate));
			queryPriceHistory(startTime, endTime, callback);
		},function(err){
			if(err) {
					context.fail("Failed to process Kinesis event" + err);
			}
			else {
					context.succeed("Successfully processed Kinesis event");
			}
		});
	}else{
		console.log('Triggered by CloudWatch');
		var endTime = new Date()
		endTime.setHours(endTime.getHours() - 1);
		endTime.setMinutes(59);
		endTime.setSeconds(59);
		endTime.setMilliseconds(999)
		var startTime = new Date(endTime);
		startTime.setMinutes(0);
		startTime.setSeconds(0);
		startTime.setMilliseconds(0);
		queryPriceHistory(startTime, endTime, function(err){
			if(err) {
				console.log("Failed to process CloudWatch event" + err);
				context.fail("Failed to process CloudWatch event" + err);
			}
			else {
				console.log("Successfully processed CloudWatch event");
				context.succeed("Successfully processed CloudWatch event");
			}
		});
	}
};

function split(array, size){
	var chunks = [];
  	while (array.length) {
    	chunks.push(array.splice(0, size));
  	}
  	return chunks;
}

function queryPriceHistory(startTime, endTime, callback){
	var instanceTypes = awsConstants.EC2_INSTANCE_TYPES;
	var instanceTypeBatches = split(instanceTypes, 4);
	var ec2 = new AWS.EC2({region: awsRegion});
	var awsRegion = process.env.AWS_REGION;
	var outputStream = process.env.OutputStream;
	console.log('Querying for price history between %s and %s in %d batches', startTime.toISOString(), endTime.toISOString(), instanceTypeBatches.length);
	async.each(instanceTypeBatches, function(instanceTypeBatch, callback){
        var params = {
			EndTime: endTime, 
			InstanceTypes: instanceTypeBatch, 
			ProductDescriptions: [
				"Linux/UNIX (Amazon VPC)"
			], 
			StartTime: startTime
		};
		ec2.describeSpotPriceHistory(params, function(err, data) {
			if (err){
				console.log(err, err.stack);
			}
			else {
				console.log(data);
				var batchRecord = new PriceHistoryBatch(startTime.toISOString(), endTime.toISOString(), data.SpotPriceHistory);
				var partitionKey = crypto.createHash('md5').update(new Date().toISOString()).digest('hex');
				awsHelpers.sendDataToKinesis(awsRegion, outputStream, partitionKey, JSON.stringify(batchRecord), callback);
			}
		});
    },function(err){
        if(err) {
			callback(err);
        }
        else {
            callback(null);
        }
    });
}
