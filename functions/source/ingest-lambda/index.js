var AWS = require('aws-sdk');
var async = require('async');
var awsHelpers = require('aws-helpers');
var crypto = require('crypto');
var PriceHistoryBatch = require('price-history-batch');
var response = require('cfn-response');

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
	} else if (event.StackId) {
		console.log('Triggered by CloudFormation.  Backfilling 30 days of data.');
		var endTime = new Date();
		var startTime = new Date(endTime.getTime()-3600*24*1000*30);
		queryPriceHistory(startTime, endTime, function(err){
			if(err) {
				console.log("Failed to process CloudFormation event" + err);
				response.send(event, context, response.FAILED);
			}
			else {
				console.log("Successfully processed CloudFormation event");
				response.send(event, context, response.SUCCESS);
			}
		});
	} else {
		console.log('Triggered by CloudWatch');
		var endTime = new Date()
		endTime.setDate(endTime.getDate() - 1);
		endTime.setHours(23);
		endTime.setMinutes(59);
		endTime.setSeconds(59);
		endTime.setMilliseconds(999)
		var startTime = new Date(endTime);
		startTime.setHours(0);
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
	var instanceTypes = ["d2.2xlarge",
"m3.large",
"c3.xlarge",
"c3.2xlarge",
"c5.9xlarge",
"i3.2xlarge",
"r3.4xlarge",
"r4.16xlarge",
"t2.large",
"m3.2xlarge",
"r4.large",
"c5.18xlarge",
"r3.2xlarge",
"r4.4xlarge",
"c3.large",
"c4.8xlarge",
"t2.2xlarge",
"m4.16xlarge",
"r4.8xlarge",
"p2.xlarge",
"i3.xlarge",
"c3.8xlarge",
"c4.xlarge",
"t2.micro",
"c3.4xlarge",
"m4.large",
"c5.4xlarge",
"t2.medium",
"c5.large",
"i2.8xlarge",
"d2.4xlarge",
"t2.xlarge",
"d2.8xlarge",
"c4.4xlarge",
"r4.2xlarge",
"i3.16xlarge",
"x1.32xlarge",
"c5.2xlarge",
"m4.xlarge",
"d2.xlarge",
"c4.2xlarge",
"c5.xlarge",
"t2.small",
"p2.16xlarge",
"r4.xlarge",
"m4.10xlarge",
"r3.large",
"i2.xlarge",
"m3.medium",
"i2.4xlarge",
"r3.xlarge",
"c4.large",
"p2.8xlarge",
"m4.2xlarge",
"i2.2xlarge",
"x1.16xlarge",
"i3.large",
"r3.8xlarge",
"i3.4xlarge",
"m4.4xlarge",
"i3.8xlarge",
"m3.xlarge"];
	var instanceTypeBatches = split(instanceTypes, 4);
	var ec2 = new AWS.EC2({region: awsRegion});
	var awsRegion = process.env.AWS_REGION;
	var outputStream = process.env.OutputStream;
	console.log('Querying for price history between %s and %s in %d batches', startTime.toISOString(), endTime.toISOString(), instanceTypeBatches.length);
	async.each(instanceTypeBatches, getSpotPrices ,function(err) {
        if(err) {
			callback(err);
        }
        else {
            callback(null);
        }
    });
    
    function getSpotPrices(instanceTypeBatch, callback, nextToken) {
	    var params = {
			EndTime: endTime, 
			InstanceTypes: instanceTypeBatch, 
			ProductDescriptions: [
				"Linux/UNIX (Amazon VPC)"
			], 
			StartTime: startTime
		};
		if(nextToken) {
			params.NextToken = nextToken;
		}
		ec2.describeSpotPriceHistory(params, function(err, data) {
			if (err){
				console.log(err, err.stack);
			}
			else {
				console.log(data);
				var batchRecord = new PriceHistoryBatch(startTime.toISOString(), endTime.toISOString(), data.SpotPriceHistory);
				var partitionKey = crypto.createHash('md5').update(new Date().toISOString()).digest('hex');
				awsHelpers.sendDataToKinesis(awsRegion, outputStream, partitionKey, JSON.stringify(batchRecord), function(err, result) {
					if(data.NextToken) {
						console.log("Getting the next batch of results for instance type batch ", instanceTypeBatch)
						getSpotPrices(instanceTypeBatch, callback, data.NextToken);
					} else {
						callback(err, result);
					}
				});
			}
		});
	}
}

