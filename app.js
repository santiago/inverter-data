const path = require('path');
const fs = require('fs');
const logParser = require('./parser');
const $ = require('async');

var logsDir = path.resolve(process.cwd(), './logs');

setInterval(run, 1 * 60 * 1000); // Run every 2 minutes

// Read logs available and apply parser to each
// concurrently
function run() {
  $.each(fs.readdirSync(logsDir), parser, (err) => {
    console.log('done syncing records to DB');
  });
}

function parser(logfile, cb) {
  const file = path.join(logsDir, logfile)
  logParser(file, cb);
}
