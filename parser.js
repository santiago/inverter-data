const fs = require('fs');
const path = require('path');
const $ = require('async');
const mysql = require('mysql');

const headers = [ 'Interval', 'DateTime', 'Serial', 'P_AC', 'E_DAY', 'T_WR', 'U_AC', 'U_AC_1', 'U_AC_2', 'U_AC_3', 'I_AC', 'F_AC', 'U_DC_1', 'I_DC_1', 'U_DC_2', 'I_DC_2', 'U_DC_3', 'I_DC_3', 'S', 'E_WR', 'M_MR', 'I_AC_1', 'I_AC_2', 'I_AC_3', 'P_AC_1', 'P_AC_2', 'P_AC_3', 'F_AC_1', 'F_AC_2', 'F_AC_3', 'R_DC', 'PC', 'PCS', 'PCS_LL', 'COS_PHI', 'COS_PHI_LL', 'S_COS_PHI', 'Current_Day_Energy', 'Current_Day_Offset', 'ccEnergyOfDay_WithoutOffset' ];
const E_WR = headers.indexOf('E_WR');
const DateTime = headers.indexOf('DateTime');
const Serial = headers.indexOf('Serial');

loadErrors((err) => {
  // console.log(errors);
});

module.exports = logParser;

function logParser(logfile, done) {
  const self = this;
  this.db = getDb();

  const _done = (err)=> { self.db.destroy(); done(err); }

  fs.readFile(logfile, 'utf8', (err, data) => {
    var isLog = false;
    var isHeader = false;

    const lines = data.split('\n').filter(l => {
      if(isHeader) { isHeader = false; isLog = true; return; }
      if(l.match(/^\[wr\]$/)) { isHeader = true }
      if(l.match(/^\[wr_end/)) { isLog = false }
      return isLog;
    });
    // Read each line concurrently and save to DB
    $.each(lines, parseAndSave.bind(this), _done);
  });
}

function parseAndSave(r, cb) {
  var values = r.split(';');
  // If the value in the
  if(parseInt(values[E_WR])) { sendErrorEmail(values[errorIndex]); }

  // Second column is a date time string so we need to quote it
  values.splice(1, 1, `"${values[DateTime]}"`);
  // Third column `Serial` is a code so we need to quote it
  values.splice(2, 1, `"${values[Serial]}"`);
  const insert = `INSERT INTO inverterdata (\`${headers.join('\`,\`')}\`, ID)
    VALUES(${values.join(',')}, NULL);`;

  this.db.query(insert, (err) => {
    if(err) { console.log(err); return cb(); }
    cb();
  });
}

// Manage error in panel
var errors = {};

function loadErrors(cb) {
  const db = getDb();

  db.query('SELECT ID, Description FROM errorcodes', (err, results) => {
    if(err) { console.log(err); return cb(err); }
    results.forEach((r) => {
      errors[r.ID] = r.Description;
    });

    db.destroy();
    cb();
  });
}

// Email facilities
const nodemailer = require('nodemailer');
// const transporter = nodemailer.createTransport();

const errorEmailFrom = 'sender@address';
const errorEmailTo = 'receiver@address';
const errorEmailSubject = 'An error occurred in panel operation #PANEL_ID';

function sendErrorEmail(error) {
  console.log('sendErrorEmail', error);
  /*transporter.sendMail({
     from: errorEmailFrom,
     to: errorEmailFrom,
     subject: errorEmailSubject,
     html: `<b>${errorEmailSubject}</b>`,
     text: errorEmailSubject
  });*/
}

function getDb() {
  return mysql.createConnection({
    host     : 'localhost',
    user     : 'shopkins_backend',
    password : 'Nnd3l9&7',
    database : 'shopkins_backend'
  });
}
