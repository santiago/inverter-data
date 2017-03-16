const path = require('path');
const fs = require('fs');
const logParser = require('./parser');
const $ = require('async');

var logsDir = path.resolve(process.cwd(), 'logs');

setInterval(run, 0.5 * 60 * 1000); // Run every 2 minutes

// Read logs available and apply parser to each
// concurrently
function run() {
  var logfiles = fs.readdirSync(logsDir);
  if(!logfiles.length) { return console.log('No logs available'); }
  $.each(logfiles, parser, (err) => {
    console.log('done syncing records to DB');
  });
}

function parser(logfile, cb) {
  const file = path.join(logsDir, logfile);
  logParser(file, cb);
}
