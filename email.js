// Email facilities
const nodemailer = require('nodemailer');
const transporter = nodemailer.createTransport({
  sendmail: true,
  newline: 'unix',
  path: '/usr/sbin/sendmail'
});


module.exports = queue;

var msgBuffer = [];

function queue(msg) {
  msgBuffer.push(msg);
}

function checkForMsgs() {
  if(!msgBuffer.length) {
    return setTimeout(() => {
      process.nextTick(checkForMsgs);
    }, 60 * 1000); // Check for new msgs to email every minute
  }

  var enqueued = msgBuffer.slice();
  msgBuffer = [];
  $.applyEachSeries(enqueued, sendEmail, checkForMsgs);
}

function sendEmail(msg, cb) {

  transporter.sendMail(msg, (err, info) => {
    if(err) { console.log(`Error sending email: ${err}`); return cb(); }
    console.log(info);
    cb();
  });
}
