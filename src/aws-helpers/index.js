var AWS = require('aws-sdk');
var async = require('async');

module.exports = {
	sendDataToFirehose: function (awsRegion, firehoseStream, data, callback) {
		var firehose = new AWS.Firehose({region: awsRegion});
		var params = {
		    DeliveryStreamName: firehoseStream, 
		    Record: {Data: data }

		};
		firehose.putRecord(params, function(err, data) {
		    if (err){
		        console.log("Failed to insert data onto Firehose: ", JSON.stringify(err));
		        callback(err);  
		    } 
		    else{
				console.log("Successfully placed data in onto the following firehose stream: " + firehoseStream);
				callback(null);
		    }
		});
	},
	sendDataToKinesis: function (awsRegion, outputStream, partitionKey, data, callback) {
	    var kinesis = new AWS.Kinesis({region: awsRegion});
	    var params = {
	       Data: data,
	       PartitionKey:  partitionKey,
	       StreamName: outputStream
	    };
	    kinesis.putRecord(params, function(err, data) {
	        if (err) {
	            console.log("Failed to insert data onto Kinesis: ", JSON.stringify(err));
	            callback(err);
	        }
	        else{
				console.log("Successfully placed data in onto the following stream: " + outputStream);
				callback(null);
	        }
	    });
  	},
  	sendDataToS3: function (awsRegion, bucketName, key, data, callback) {
	    var s3 = new AWS.S3({region: awsRegion});
	  	var params = {
	    		Body: data, 
	    		Bucket: bucketName, 
	    		Key: key, 
	    		ServerSideEncryption: "AES256"
	   	};
	   	s3.putObject(params, function(err, data) {
	   		if(err){
	          	console.log("Failed to insert data into S3: ", JSON.stringify(err));
	          	callback(err);
	        }
	        else{
				console.log("Successfully placed data in S3 at s3://" + bucketName + "/" + key);
				callback(null);
	        }
   		});
  	},

	listAllS3Objects: function (awsRegion, bucketName, prefix, allS3Objects, token, callback)
	{
    	var s3 = new AWS.S3({region: awsRegion});

		var opts = { 
			Bucket: bucketName,
			Prefix: prefix 
		};
		if(token) opts.ContinuationToken = token;

		s3.listObjectsV2(opts, function(err, data){
			if (err) console.log(err, err.stack);
			else {
			    allS3Objects = allS3Objects.concat(data.Contents);

			    if(data.IsTruncated)
			        listAllS3Objects(s3, bucketName, prefix, allS3Objects, data.NextContinuationToken, callback);
			    else
			        callback(allS3Objects);
			}
		});
	},

	readAllS3Objects: function (awsRegion, bucketName, allS3Objects, callback)
	{   
    	var s3 = new AWS.S3({region: awsRegion});

		var readS3Object = this.readS3Object;
	    var readAsync = function(s3Object, callback){
	        readS3Object(awsRegion, bucketName, s3Object.Key, callback);
	    }

	    async.concat(allS3Objects, readAsync, function(err, objects){
	       if (err){
	            console.log(err, err.stack);
	            callback(err);
	        }else{
	            callback(null, objects);
	        }
	    });
	},

	readS3Object: function (awsRegion, bucketName, key, callback)
	{ 
		var s3 = new AWS.S3({region: awsRegion});

	    s3.getObject({ Bucket: bucketName, Key: key }, function(err, data)
	    {
	        if (err){
	            console.log(err, err.stack);
	            callback(err);
	        }else{
	            callback(null, data.Body.toString());
	        }  
	    });
	}
}