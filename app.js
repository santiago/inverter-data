const path = require('path');
const fs = require('fs');
const logParser = require('./parser');
const $ = require('async');

var logsDir = path.resolve(process.cwd(), './logs');

setInterval(run, 1 * 60 * 1000); // Run every 2 minutes

// Read logs available and apply parser to each
// concurrently
<<<<<<< HEAD
function run() {
  $.each(fs.readdirSync(logsDir), parser, (err) => {
    console.log('done syncing records to DB');
  });
}
=======
$.each(fs.readdirSync(logsDir), parser, (err) => {
  console.log('done syncing records to DB');
});
>>>>>>> 8307c381a9f018e401ef7c15a19f89efb05eb374

function parser(logfile, cb) {
  const file = path.join(logsDir, logfile)
  logParser(file, cb);
}
