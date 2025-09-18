// start.js
const { spawn } = require('child_process');

// Start Quickbase sync
spawn('node', ['sync-quickbase.js'], { stdio: 'inherit' });

// Start Geckoboard push
spawn('node', ['push-to-geckoboard.js', '--continuous'], { stdio: 'inherit' });

console.log('🚀 Both services started');