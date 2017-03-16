const fs = require('fs');
const path = require('path');
const $ = require('async');
const mysql = require('mysql');

const headers = [ 'Interval', 'DateTime', 'Serial', 'P_AC', 'E_DAY', 'T_WR', 'U_AC', 'U_AC_1', 'U_AC_2', 'U_AC_3', 'I_AC', 'F_AC', 'U_DC_1', 'I_DC_1', 'U_DC_2', 'I_DC_2', 'U_DC_3', 'I_DC_3', 'S', 'E_WR', 'M_MR', 'I_AC_1', 'I_AC_2', 'I_AC_3', 'P_AC_1', 'P_AC_2', 'P_AC_3', 'F_AC_1', 'F_AC_2', 'F_AC_3', 'R_DC', 'PC', 'PCS', 'PCS_LL', 'COS_PHI', 'COS_PHI_LL', 'S_COS_PHI', 'Current_Day_Energy', 'Current_Day_Offset', 'ccEnergyOfDay_WithoutOffset' ];
const E_WR = headers.indexOf('E_WR');
const DateTime = headers.indexOf('DateTime');
const Serial = headers.indexOf('Serial');
const Current_Day_Energy = headers.indexOf('Current_Day_Energy');

var registry = {};
var sunrise, sunset;

loadErrors((err) => {
  // console.log(errors);
});

loadDaylightHours(() => {
});

module.exports = logParser;

function logParser(logfile, done) {
  const self = this;
  this.db = getDb();

  const _done = (err)=> {
    // It's done parsing the `logfile`.
    // Close the DB handle.
    self.db.destroy();
    // Archive the parsed logfile
    archiveLog(logfile, () => {
      done(err);
    });
  }

  fs.readFile(logfile, 'utf8', (err, data) => {
    var isLog = false;
    var isHeader = false;

    const lines = data.split('\n').filter(l => {
      if(isHeader) { isHeader = false; isLog = true; return; }
      if(l.match(/^\[wr\]$/)) { isHeader = true }
      if(l.match(/^\[wr_end/)) { isLog = false }
      return l && isLog;
    });

    // Read each line concurrently and save to DB
    $.each(lines, processRecord.bind(this), _done);
  });
}

function processRecord(r, cb) {
  var values = r.split(';');

  // If the value in the E_WR is different than zero, then
  // notify the error via email
  if(parseInt(values[E_WR])) { sendErrorEmail(values[E_WR]); }

  const serial = values[Serial];
  const time = (new Date(values[DateTime])).getTime();
  const currentEnergy = parseInt(values[Current_Day_Energy]);

  // If processed record is still not in registry, add it
  registry[serial] || (registry[serial] = { time, currentEnergy });

  // If we're still in daylight hours check
  // whether there was power collected in the last hour.
  if(isDaylightNow()) {
    const lastTime = registry[serial].time;
    // Check every 1 hour
    if(time - lastTime > 1 * 60 * 60 * 1000) {
      const lastEnergy = registry[serial].currentEnergy;
      // If during daylight the panel collected no
      // power in the last hour, notify via email
      if(!(currentEnergy > lastEnergy)) {
        sendErrorEmail('999');
      }
      registry[serial] = { time, currentEnergy };
    }
  }

  // Parse the record and save to DB
  parseAndSave(values, cb);
  setTimeout(cb, 10);
}

function parseAndSave(values, cb) {

  // Second column is a date time string so we need to quote it
  values.splice(1, 1, `"${values[DateTime]}"`);
  // Third column `Serial` is a string code so we need to quote it
  values.splice(2, 1, `"${values[Serial]}"`);
  const insert = `INSERT INTO inverterdata (\`${headers.join('\`,\`')}\`, ID)
    VALUES(${values.join(',')}, NULL);`;

  this.db.query(insert, (err) => {
    if(err) { console.log(err); return cb(); }
    cb();
  });
}

// Manage error in solar panel
var errors = {};

function loadErrors(cb) {
  const db = getDb();

  db.query('SELECT ID, Description FROM errorcodes', (err, results) => {
    if(err) { console.log(err); return cb(err); }
    results.forEach((r) => {
      errors[r.ID] = r.Description;
    });
    errors['999'] = 'Has not collected power in the last hour.';

    db.destroy();
    cb();
  });
}

function loadDaylightHours(cb) {
  // db.query('SELECT Date, Dawn, Sunrise, Sunset FROM daylighthours WHERE ', (err, results) => {
  //   results.
  // })
  setTimeout(() => {
    sunrise = '6:00';
    sunset = '19:00';

    const sunriseHours = parseInt(sunrise.split(':').shift());
    const sunriseMinutes = parseInt(sunrise.split(':').pop());
    var sunriseDate = new Date();
    sunriseDate.setHours(sunriseHours);
    sunriseDate.setMinutes(sunriseMinutes);

    const sunsetHours = parseInt(sunset.split(':').shift());
    const sunsetMinutes = parseInt(sunset.split(':').pop());
    var sunsetDate = new Date();
    sunsetDate.setHours(sunsetHours);
    sunsetDate.setMinutes(sunsetMinutes);

    sunrise = sunriseDate.getTime();
    sunset = sunsetDate.getTime();

    cb();
  }, 10);
}

/*
 * archiveLog: send the parsed log to an archive directory
 */
function archiveLog(logfile, cb) {
  const file = logfile.split('/').pop();
  const archiveDir = path.resolve(process.cwd(), 'archive');
  const newPath = path.join(archiveDir, file);
  fs.link(logfile, newPath, () => {
    fs.unlinkSync(logfile);
    cb();
  });
}

// Email facilities
const nodemailer = require('nodemailer');
const transporter = nodemailer.createTransport({
  sendmail: true,
  newline: 'unix',
  path: '/usr/sbin/sendmail'
});

const errorEmailFrom = 'sender@address';
const errorEmailTo = 'receiver@address';
const errorEmailSubject = 'An error occurred in panel operation #PANEL_ID';

function sendErrorEmail(errorCode) {
  const code = parseInt(errorCode);
  code && console.log('sendErrorEmail', errors[errorCode]);
  transporter.sendMail({
     from: errorEmailFrom,
     to: errorEmailFrom,
     subject: errorEmailSubject,
     html: `<b>${errorEmailSubject}</b>`,
     text: errorEmailSubject
  });
}

function getDb() {
  return mysql.createConnection({
    host     : 'localhost',
    user     : 'shopkins_backend',
    password : 'Nnd3l9&7',
    database : 'shopkins_backend'
  });
}

function isDaylightNow() {
  return Date.now() < sunset;
}
