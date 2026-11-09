/**
 * Standalone Video Downloader with Real-Time Progress for GitHub Actions Runner
 * Runs yt-dlp (with aria2c multi-connection support and fallback), parses real-time
 * download percentage, and reports progress to ApexStream every 4-5 seconds.
 */
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const SOURCE_URL = process.env.SOURCE_URL || '';
const FALLBACK_URL = process.env.FALLBACK_URL || '';
const OUTPUT_FILE = process.env.OUTPUT_FILE || 'video.mkv';
const JOB_ID = process.env.JOB_ID || '';
const RAW_CALLBACK = process.env.CALLBACK_URL || '';
const CALLBACK_URL = RAW_CALLBACK.replace(/\/api\/.*$/, '').replace(/\/+$/, '');
const INTERNAL_TOKEN = process.env.INTERNAL_API_TOKEN || process.env.INTERNAL_TOKEN || '';
const GITHUB_RUN_ID = process.env.GITHUB_RUN_ID || '';

if (!SOURCE_URL) {
  console.error('[Downloader] Error: Missing SOURCE_URL');
  process.exit(1);
}

// Throttled progress reporting
let lastReportedPct = -1;
let lastReportTime = 0;

async function reportProgress(jobId, status, progressPct, force = false) {
  if (!jobId || !CALLBACK_URL || !INTERNAL_TOKEN) return;
  const now = Date.now();
  const pct = Math.max(0, Math.min(100, Math.round(progressPct)));

  if (!force && pct === lastReportedPct) return;
  if (!force && pct < 100 && (now - lastReportTime < 4000) && (pct - lastReportedPct < 3)) {
    return;
  }

  lastReportedPct = pct;
  lastReportTime = now;

  const payload = JSON.stringify({
    job_id: jobId,
    status,
    progress_pct: pct,
    gh_run_id: GITHUB_RUN_ID || undefined
  });

  const url = `${CALLBACK_URL}/api/v1/internal/ingest/progress`;
  console.log(`[Downloader] Progress: ${status} -> ${pct}%`);

  try {
    const u = new URL(url);
    const client = u.protocol === 'http:' ? http : https;
    await new Promise((resolve) => {
      const req = client.request({
        hostname: u.hostname,
        port: u.port || (u.protocol === 'http:' ? 80 : 443),
        path: u.pathname,
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${INTERNAL_TOKEN}`,
          'Content-Type': 'application/json',
          'User-Agent': 'ApexStream-Downloader/1.0',
          'Content-Length': Buffer.byteLength(payload),
        },
        timeout: 5000
      }, res => {
        res.resume();
        resolve(res.statusCode);
      });
      req.on('error', (e) => {
        // Non-blocking logger
        resolve(null);
      });
      req.write(payload);
      req.end();
    });
  } catch (err) {
    // silently continue download
  }
}

function runDownloadAttempt(url, useAria2 = true) {
  return new Promise((resolve) => {
    console.log(`[Downloader] Starting download from ${url} (aria2: ${useAria2})...`);
    
    const isGoogle = url.includes('googleusercontent.com') || url.includes('drive.google.com');
    const args = ['--newline', '--no-colors'];
    if (useAria2) {
      args.push('--external-downloader', 'aria2c');
      args.push('--external-downloader-args', isGoogle ? '-s 1 -x 1' : '-x 16 -s 16 -k 1M -j 16');
    }
    args.push(url, '-o', OUTPUT_FILE);

    const proc = spawn('yt-dlp', args, {
      env: { ...process.env, PYTHONUNBUFFERED: '1' }
    });

    const handleOutput = (chunk) => {
      const text = chunk.toString();
      // Print to stdout for GitHub logs
      process.stdout.write(text);

      // Match yt-dlp: "[download]  45.2% of 265MiB" or aria2c: "[#2c9d64 120MiB/265MiB(45%)]"
      const lines = text.split(/[\r\n]+/);
      for (const line of lines) {
        const m = line.match(/(?:\[download\]\s+|[\(\[])\s*([\d\.]+)%/i);
        if (m) {
          const val = parseFloat(m[1]);
          if (!isNaN(val)) {
            reportProgress(JOB_ID, 'downloading', val);
          }
        }
      }
    };

    proc.stdout.on('data', handleOutput);
    proc.stderr.on('data', handleOutput);

    proc.on('close', (code) => {
      console.log(`[Downloader] Attempt exited with code ${code}`);
      resolve(code === 0);
    });

    proc.on('error', (err) => {
      console.error(`[Downloader] Process error:`, err);
      resolve(false);
    });
  });
}

async function main() {
  await reportProgress(JOB_ID, 'downloading', 0, true);

  // Attempt 1: Aria2c accelerated on primary URL
  let success = await runDownloadAttempt(SOURCE_URL, true);

  // Attempt 2: Native yt-dlp on primary URL
  if (!success) {
    console.warn('[Downloader] Aria2c download failed, retrying with native yt-dlp...');
    success = await runDownloadAttempt(SOURCE_URL, false);
  }

  // Attempt 3: Fallback URL if provided
  if (!success && FALLBACK_URL) {
    console.warn(`[Downloader] Primary URL failed, retrying with fallback URL: ${FALLBACK_URL}...`);
    success = await runDownloadAttempt(FALLBACK_URL, false);
  }

  if (!success || !fs.existsSync(OUTPUT_FILE) || fs.statSync(OUTPUT_FILE).size === 0) {
    console.error('[Downloader] All download attempts failed!');
    process.exit(1);
  }

  const finalSize = fs.statSync(OUTPUT_FILE).size;
  console.log(`[Downloader] Download successfully completed! File size: ${(finalSize / 1024 / 1024).toFixed(2)} MB`);
  await reportProgress(JOB_ID, 'downloading', 100, true);
  process.exit(0);
}

main().catch((err) => {
  console.error('[Downloader] Fatal error:', err);
  process.exit(1);
});

