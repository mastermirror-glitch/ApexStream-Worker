/**
 * Standalone LuluStream Uploader for GitHub Actions Runner
 * Uses native https to obtain the upload server URL, then streams via curl
 * with progress tracking and output parsing.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const RAW_CALLBACK   = process.env.CALLBACK_URL || '';
const CALLBACK_URL   = RAW_CALLBACK.replace(/\/api\/.*$/, '').replace(/\/+$/, '');
const INTERNAL_TOKEN = process.env.INTERNAL_API_TOKEN || process.env.INTERNAL_TOKEN || '';

async function reportProgress(jobId, status, progressPct) {
  if (!CALLBACK_URL || !INTERNAL_TOKEN) return;
  const ghRunId = process.env.GITHUB_RUN_ID;
  const payload = JSON.stringify({
    job_id: jobId,
    status,
    progress_pct: progressPct,
    gh_run_id: ghRunId || undefined
  });
  const url = `${CALLBACK_URL}/api/v1/internal/ingest/progress`;

  for (let i = 0; i <= 5; i++) {
    try {
      const statusCode = await new Promise((resolve, reject) => {
        const u = new URL(url);
        const req = https.request({
          hostname: u.hostname,
          path: u.pathname,
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${INTERNAL_TOKEN}`,
            'Content-Type': 'application/json',
            'User-Agent': 'ApexStream-Internal-Worker/1.0',
            'Content-Length': Buffer.byteLength(payload),
          }
        }, res => { res.resume(); resolve(res.statusCode); });
        req.on('error', reject);
        req.write(payload);
        req.end();
      });

      if (statusCode >= 200 && statusCode < 300) return;
      if (statusCode === 404 || statusCode >= 500) {
        console.error(`[Prog API] Status ${statusCode}. Retrying ${i + 1}/5...`);
      } else {
        return;
      }
    } catch (e) {
      console.error(`[Prog API] Error: ${e.message}. Retrying ${i + 1}/5...`);
    }
    if (i < 5) await new Promise(r => setTimeout(r, 5000));
  }
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'ApexStream-Ingest/2.0' } }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`Failed to parse response: ${data}`));
        }
      });
    }).on('error', reject);
  });
}

async function upload() {
  const filePath = process.env.FILE_PATH;
  const apiKey   = process.env.LULUSTREAM_KEY || process.env.LULUSTREAM_API_KEY || process.env.STREAMHG_KEY || process.env.STREAMHG_API_KEY;
  const jobId    = process.env.JOB_ID;
  const folderId = process.env.LULUSTREAM_FOLDER_ID || process.env.STREAMHG_FOLDER_ID || process.env.SEEK_FOLDER_ID || '0';
  const fileTitle = process.env.FILE_TITLE || (filePath ? path.basename(filePath) : '');

  if (!filePath || !apiKey) {
    console.error('Missing FILE_PATH or LULUSTREAM_KEY');
    process.exit(1);
  }

  if (!fs.existsSync(filePath)) {
    console.error(`File does not exist: ${filePath}`);
    process.exit(1);
  }

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;
  const fileName = path.basename(filePath);

  console.log(`[LuluStream] Initializing upload for: ${fileName} (${(fileSize / 1024 / 1024).toFixed(1)} MB)`);

  const MAX_ATTEMPTS = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      console.log(`[LuluStream] Requesting upload server endpoint (Attempt ${attempt}/${MAX_ATTEMPTS})...`);
      const serverInfo = await fetchJson(`https://api.lulustream.com/api/upload/server?key=${apiKey}`);

      if (serverInfo.status !== 200 || !serverInfo.result) {
        throw new Error(`Failed to retrieve LuluStream upload server: ${serverInfo.msg || 'Unknown error'}`);
      }

      const uploadUrl = serverInfo.result;
      console.log(`[LuluStream] Upload server assigned: ${uploadUrl}`);

      // Spawn curl with streaming multipart upload using HTTP/1.1
      const escapedPath = filePath.replace(/"/g, '\\"');
      const args = [
        '-s', '-S',
        '-#',
        '--http1.1',
        '--connect-timeout', '60',
        '--retry', '2',
        '--retry-delay', '3',
        '-F', `key=${apiKey}`,
        '-F', `file=@"${escapedPath}"`,
      ];

      if (fileTitle) {
        args.push('-F', `file_title=${fileTitle}`);
      }
      if (folderId && folderId !== '0') {
        args.push('-F', `fld_id=${folderId}`);
      }

      args.push(uploadUrl);

      await reportProgress(jobId, 'uploading', 0).catch(() => {});

      console.log(`[LuluStream] Uploading via curl to ${uploadUrl}...`);

      let stdout = '';
      let stderrText = '';
      let lastReportedPct = -1;
      let lastReportTime = 0;

      const curlProc = spawn('curl', args);

      curlProc.stdout.on('data', chunk => {
        stdout += chunk.toString();
      });

      curlProc.stderr.on('data', chunk => {
        const text = chunk.toString();
        stderrText += text;
        let match = text.match(/([\d\.]+)%/);

        if (!match) {
          const lines = text.split(/[\r\n]+/);
          for (const l of lines) {
            const col = l.trim().match(/^(\d{1,3})\s+[\d\.]+[kMG]?/);
            if (col && parseInt(col[1], 10) <= 100) {
              match = col;
              break;
            }
          }
        }

        if (match) {
          const pct = Math.floor(parseFloat(match[1]));
          const now = Date.now();
          if (pct > lastReportedPct && (now - lastReportTime >= 4000 || pct === 100 || pct >= lastReportedPct + 3)) {
            lastReportedPct = pct;
            lastReportTime = now;
            console.log(`[LuluStream] Upload Progress: ${pct}%`);
            reportProgress(jobId, 'uploading', pct).catch(() => {});
          }
        }
      });

      const exitCode = await new Promise((resolve, reject) => {
        curlProc.on('close', resolve);
        curlProc.on('error', reject);
      });

      if (exitCode !== 0) {
        const errorLines = stderrText
          .split(/[\r\n]+/)
          .map(l => l.trim())
          .filter(l => l && !l.includes('%') && !l.match(/^[#=\s]+$/));
        const cleanErr = errorLines.pop() || `curl exited with status ${exitCode}`;
        throw new Error(cleanErr);
      }

      console.log('[LuluStream] Raw upload response:', stdout.trim());

      const rawOutput = stdout.trim();
      if (rawOutput.includes('413 Request Entity Too Large') || rawOutput.includes('413 Payload Too Large')) {
        throw new Error('LuluStream upload failed: File too large (HTTP 413 Request Entity Too Large on storage server)');
      }
      if (rawOutput.includes('500 ERROR') || rawOutput.includes('500 Internal Server Error')) {
        throw new Error('LuluStream upload failed: Storage server failure (HTTP 500 Internal Server Error)');
      }

      let resJson;
      try {
        resJson = JSON.parse(rawOutput);
      } catch (e) {
        const titleMatch = rawOutput.match(/<title>([^<]+)<\/title>/i);
        const htmlTitle = titleMatch ? titleMatch[1].trim() : '';
        throw new Error(`Invalid JSON received from LuluStream: ${htmlTitle || rawOutput.slice(0, 150)}`);
      }

      if (resJson.status !== 200 || !resJson.files || !resJson.files[0]?.filecode) {
        const fileStatus = resJson.files?.[0]?.status || '';
        const isLimit = fileStatus.includes('too many files today') || (resJson.msg && resJson.msg.includes('too many files'));
        let errDetail = '';
        if (isLimit) {
          errDetail = 'LULUSTREAM_LIMIT_REACHED: too many files today (daily upload limit exceeded)';
        } else if (fileStatus && fileStatus !== 'OK') {
          errDetail = fileStatus;
        } else if (resJson.msg && resJson.msg !== 'OK') {
          errDetail = resJson.msg;
        } else {
          errDetail = 'LuluStream did not return a filecode (duplicate upload or server processing hold)';
        }
        const err = new Error(`LuluStream upload failed: ${errDetail}`);
        if (isLimit) {
          err.isDailyLimit = true;
        }
        throw err;
      }

      const fileInfo = resJson.files[0];
      const filecode = fileInfo.filecode;
      const embedUrl = `https://luluvid.com/e/${filecode}`;

      console.log(`[LuluStream] Upload complete! File Code: ${filecode} | Embed: ${embedUrl}`);

      fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
        job_id: jobId,
        status: 'ready',
        provider: 'lulustream',
        gh_run_id: process.env.GITHUB_RUN_ID || undefined,
        filecode: filecode,
        videoId: filecode,
        embedUrl: embedUrl,
        filename: fileName,
        size: fileSize,
        audio_languages: process.env.AUDIO_LANGUAGES ? process.env.AUDIO_LANGUAGES.split(',').filter(Boolean) : [],
        subtitle_languages: process.env.SUBTITLE_LANGUAGES ? process.env.SUBTITLE_LANGUAGES.split(',').filter(Boolean) : []
      }));

      return;

    } catch (err) {
      lastError = err;
      console.warn(`[LuluStream] Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${err.message}`);

      const isDailyLimit = err.isDailyLimit ||
        err.message.includes('LULUSTREAM_LIMIT_REACHED') ||
        err.message.includes('too many files today');

      if (isDailyLimit) {
        console.error('[LuluStream] ⛔ Daily upload limit reached on this API key. Aborting upload retries immediately.');
        break;
      }

      if (attempt < MAX_ATTEMPTS) {
        console.log('[LuluStream] Waiting 5 seconds before retrying with a fresh upload server...');
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }

  // If all attempts failed
  console.error('[LuluStream] All upload attempts failed:', lastError?.message);
  fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
    job_id: jobId,
    status: 'error',
    provider: 'lulustream',
    error: lastError?.message || 'Upload failed after retries'
  }));
  process.exit(1);
}

upload();

