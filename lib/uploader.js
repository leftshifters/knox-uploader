/*
 * knox-uploader
 * https://github.com/prama/knox-uploader
 *
 * Copyright (c) 2014 Prama
 * Licensed under the MIT license.
 */


'use strict';
var knox = require('knox');
var fs = require('fs');
var debug = require('debug')('s3:upload');

exports = module.exports = function upload(keys, file, done) {

  validateKeys(keys);
  validateFile(file);

  var client = knox.createClient(keys);

  fs.stat(file.path, function(err, stat){

    var s3 = client.put(file.name, {
      'Content-Length': stat.size,
      'Content-Type': file.type
    });

    console.time('Uploading');
    fs.createReadStream(file.path).pipe(s3);

    s3.on('response', function(res){
      console.timeEnd('Uploading');

      if(res.statusCode === 200) {
        done(null, file.name);
      } else {
        console.error('S3 failed %s status, filename: %s, mime: %s, size: %s ', res.statusCode, file.name, file.type, file.size);
        s3.emit('error', new Error('S3 upload failed'));
      }
    });

    s3.on('error', function(err) {
      console.error(err);
      done(err);
    });
  });
};

function validateKeys(keys) {
  if(!keys) {
    throw new Error('S3 keys are not provided');
  }

  if(!keys.key) {
    throw new Error('Key is missing');
  }

  if(!keys.secret) {
    throw new Error('S3 secret is missing');
  }

  if(!keys.bucket) {
    throw new Error('Bucket name is missing');
  }
}

function validateFile(file) {
  if(!file) {
    throw new Error('File object is not present');
  }

  if(!file.path) {
    throw new Error('File path is missing');
  }

  if(!file.name) {
    throw new Error('File name is missing');
  }

  if(!file.type) {
    throw new Error('File type is missing');
  }

  if(!file.size) {
    throw new Error('Bucket name is missing');
  }

}
