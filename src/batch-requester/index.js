"use strict";

var awsHelpers = require('aws-helpers');
var crypto = require('crypto');
var async = require('async');
var deasync = require('deasync');

class IngestRequest {

    constructor (startDate, endDate){
        this.startDate = startDate.toISOString();
        this.endDate = endDate.toISOString();
    }
}

var endDate = new Date();
endDate.setHours(endDate.getHours() - 1);
endDate.setMinutes(0);
endDate.setSeconds(0);
endDate.setMilliseconds(0);
var startDate = new Date(endDate.getTime());
startDate.setDate(startDate.getDate() - 89);
console.log("Sending requests for range: %s - %s", startDate, endDate);
var requestStartDate = new Date(startDate.getTime());

var awsRegion = "us-east-1";
var outputStream = "BatchRequestStream-dluszcz";
var sentRequests = 0;

var hoursBetween = Math.abs(endDate.getTime() - startDate.getTime()) / 36e5;

while (requestStartDate <= endDate) {
    sendRequest(requestStartDate, function(){
        sentRequests  = sentRequests + 1;
        console.log("Completed sending %d requests for range: %s - %s", sentRequests, startDate, endDate);
    });
    requestStartDate.setHours(requestStartDate.getHours()+1);
}

deasync.loopWhile(() => sentRequests < hoursBetween);

console.log("Completed sending %d requests for range: %s - %s", sentRequests, startDate, endDate);


async function sendRequest(requestStartDate, callback){
    var requestEndDate = new Date(requestStartDate.getTime());
    requestEndDate.setMinutes(59);
    requestEndDate.setSeconds(59);
    requestEndDate.setMilliseconds(999);
    var request = new IngestRequest(requestStartDate, requestEndDate);
    var partitionKey = crypto.createHash('md5').update(new Date().toISOString()).digest('hex');
    console.log("Sending request for range: %s - %s", requestStartDate.toISOString(), requestEndDate.toISOString());
    await awsHelpers.sendDataToKinesis(awsRegion, outputStream, partitionKey, JSON.stringify(request), function(err){
        if(err) {
            console.log("Failed to send request" + err);
        }else{
            console.log("Successfully sent request for range: %s - %s", requestStartDate.toISOString(), requestEndDate.toISOString());
        }
        callback();
    });
}