var http = require('http');
var fs = require('fs');
var fcsv = require("fast-csv");
var express = require('express');
var EventEmitter = require('events');
var schedule = require('node-schedule');
var tar = require('tar-fs');
var zlib = require('zlib');
var glob = require('glob');
var moveFile = require('move-file');

var nodesList = [];

var client;
var app = express(); 
var sec = 5;
var timer = new EventEmitter();
var j = schedule.scheduleJob('03 00 * * * *', function () {
    downloadCSV();
});

var week1_date = new Date();
var week2_date = new Date();
var week3_date = new Date();
var week4_date = new Date();
week1_date.setDate(week1_date.getDate()-7);
week2_date.setDate(week2_date.getDate()-14);
week3_date.setDate(week3_date.getDate()-21);
week4_date.setDate(week4_date.getDate()-28);

var saveFileName = ["../active/aot-week1.csv","../active/aot-week2.csv","../active/aot-week3.csv","../active/aot-week4.csv"];
var saveFileStreamArr = [];
saveFileStreamArr.push(fs.createWriteStream(saveFileName[0]));
saveFileStreamArr.push(fs.createWriteStream(saveFileName[1]));
saveFileStreamArr.push(fs.createWriteStream(saveFileName[2]));
saveFileStreamArr.push(fs.createWriteStream(saveFileName[3]));

var csvStreamArr = [];
csvStreamArr.push(fcsv.createWriteStream({headers: true}));
csvStreamArr.push(fcsv.createWriteStream({headers: true}));
csvStreamArr.push(fcsv.createWriteStream({headers: true}));
csvStreamArr.push(fcsv.createWriteStream({headers: true}));

csvStreamArr[0].pipe(saveFileStreamArr[0]);
csvStreamArr[1].pipe(saveFileStreamArr[1]);
csvStreamArr[2].pipe(saveFileStreamArr[2]);
csvStreamArr[3].pipe(saveFileStreamArr[3]);

var targetSensorArr = ['no2','spv1840lr5h_b'];
var targetParamArr = ['pm2_5','pm25_atm'];


function downloadCSV(){
    //download nodes file
    new Promise(function (resolve, reject) {
        var filename = "./AoT-complete-latest.tar";
        var url = 'http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/NUCWR-MUGS.complete.latest.tar';
        //"http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/AoT_Chicago.complete.latest.tar";
        
        //download AoT-complete-latest.tar file
        console.log("downloading tar..");
        //file = fs.createWriteStream(filename);
        //var request = http.get(url, function (response) {
        //    response.pipe(file);
        //});

        var file = fs.createWriteStream(filename);
        var request = http.get(url, function(response) {
            response.pipe(file);
        });
        file.on('error', function(err) { // Handle errors
            console.log(err);
        });

        file.on("finish", function () {
           //after download, unzip AoT-complete-latest.tar. 
           console.log(filename+'.tar unzip start.')
           var tarStream = fs.createReadStream(filename).pipe(tar.extract('./AoT-complete-latest'));
           tarStream.on("finish",function(){
                console.log(filename+'.tar extract complete.');
                  glob("./AoT-complete-latest/*/",function(er, files){
                    console.log(files[0]+'data.csv.gz unzip start.');
                    var gzStream = fs.createReadStream(files[0]+'data.csv.gz')
                    // Un-Gzip
                    .pipe(zlib.createGunzip())
                    // Write File
                    .pipe(fs.createWriteStream("./AoT-complete-latest/data.csv"));
                        console.log(files[0]);
                    gzStream.on("finish", function(){
                         console.log(files[0]+'data.csv extract complete.');
                        (async () => {
                                await moveFile(files[0]+'/nodes.csv', './AoT-complete-latest/nodes.csv');
                                console.log('File moved');
                                resolve(1);
                        })();
                     })
                });  
             })             
        })            
    })
    .then(function () {
        var stream = fs.createReadStream("./AoT-complete-latest/nodes.csv");
        fcsv
        .fromStream(stream, { headers: ['node_id', ,, , , , ,]})
        .on('data', function (data) {
            if (data['node_id'] != 'node_id') {
                nodesList.push(data['node_id']);
            }
        })
        .on('end',function(){
            console.log(nodesList);
            var stream = fs.createReadStream("./AoT-complete-latest/data.csv");
            fcsv
            .fromStream(stream, { headers: true})
            .on("data", function (data) {
                if (targetParamArr.includes(data['parameter']) || targetSensorArr.includes(data['sensor'])) 
                {   
                    console.log(data);
                    //write latest 1 month data
                    if(week1_date < convertTimestamptoDate(data['timestamp']))
                        csvStreamArr[0].write({Node:data['node_id'], Timestamp:data['timestamp'], Sensor:data['sensor'], Parameter:data['parameter'], Value_raw: data['value_raw'], Value_hrf: data['value_hrf']});
                    else if(week2_date < convertTimestamptoDate(data['timestamp']))
                        csvStreamArr[1].write({Node:data['node_id'], Timestamp:data['timestamp'], Sensor:data['sensor'], Parameter:data['parameter'], Value_raw: data['value_raw'], Value_hrf: data['value_hrf']});
                    else if(week3_date < convertTimestamptoDate(data['timestamp']))
                        csvStreamArr[2].write({Node:data['node_id'], Timestamp:data['timestamp'], Sensor:data['sensor'], Parameter:data['parameter'], Value_raw: data['value_raw'], Value_hrf: data['value_hrf']});
                    else if(week4_date < convertTimestamptoDate(data['timestamp']))
                        csvStreamArr[3].write({Node:data['node_id'], Timestamp:data['timestamp'], Sensor:data['sensor'],Parameter:data['parameter'], Value_raw: data['value_raw'], Value_hrf: data['value_hrf']});
                }
            })
            .on("end", function(){
                var i =0;
                for(i=0; i<4; i++){
                    csvStreamArr[i].end();
                }
            })
        });
    })
};


function convertTimestamptoDate(timestamp)
{
    //date format example: 2017/12/08 23:22:27
    timestamp=timestamp+'';
    var timeArr = timestamp.split(/[\s:/]/);
    timeArr[1] = timeArr[1]-1;
    return new Date(timeArr[0], timeArr[1], timeArr[2]);
}


app.listen(8000, function(){
    console.log("running...")
})