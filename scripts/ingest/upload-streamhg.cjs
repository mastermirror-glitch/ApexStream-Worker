/**
 * Standalone StreamHG Uploader for GitHub Actions Runner
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
  const apiKey   = process.env.STREAMHG_KEY || process.env.STREAMHG_API_KEY;
  const jobId    = process.env.JOB_ID;
  const folderId = process.env.STREAMHG_FOLDER_ID || process.env.SEEK_FOLDER_ID || '0';
  const fileTitle = process.env.FILE_TITLE || (filePath ? path.basename(filePath) : '');

  if (!filePath || !apiKey) {
    console.error('Missing FILE_PATH or STREAMHG_KEY');
    process.exit(1);
  }

  if (!fs.existsSync(filePath)) {
    console.error(`File does not exist: ${filePath}`);
    process.exit(1);
  }

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;
  const fileName = path.basename(filePath);

  console.log(`[StreamHG] Initializing upload for: ${fileName} (${(fileSize / 1024 / 1024).toFixed(1)} MB)`);

  const MAX_ATTEMPTS = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      console.log(`[StreamHG] Requesting upload server endpoint (Attempt ${attempt}/${MAX_ATTEMPTS})...`);
      const serverInfo = await fetchJson(`https://streamhgapi.com/api/upload/server?key=${apiKey}`);

      if (serverInfo.status !== 200 || !serverInfo.result) {
        throw new Error(`Failed to retrieve StreamHG upload server: ${serverInfo.msg || 'Unknown error'}`);
      }

      const uploadUrl = serverInfo.result;
      console.log(`[StreamHG] Upload server assigned: ${uploadUrl}`);

      // 2. Spawn curl with streaming multipart upload using HTTP/1.1
      // Note: Do NOT use -X POST with -F as it overrides curl's internal method state machine.
      // Force --http1.1 to avoid HTTP/2 stream multiplex resets on high-throughput video uploads.
      const args = [
        '-s', '-S',
        '-#',
        '--http1.1',
        '--connect-timeout', '60',
        '--retry', '2',
        '--retry-delay', '3',
        '-F', `key=${apiKey}`,
        '-F', `file=@${filePath}`,
      ];

      if (fileTitle) {
        args.push('-F', `file_title=${fileTitle}`);
      }
      if (folderId && folderId !== '0') {
        args.push('-F', `fld_id=${folderId}`);
      }

      args.push(uploadUrl);

      await reportProgress(jobId, 'uploading', 0).catch(() => {});

      console.log(`[StreamHG] Uploading via curl to ${uploadUrl}...`);

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
        // 1. Matches curl "-#" / "--progress-bar" (e.g. " 45.2% " or " 3% ")
        let match = text.match(/([\d\.]+)%/);

        // 2. Fallback: match curl progress-meter tabular output if present
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
            console.log(`[StreamHG] Upload Progress: ${pct}%`);
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

      console.log('[StreamHG] Raw upload response:', stdout.trim());

      // 3. Parse response
      let resJson;
      try {
        resJson = JSON.parse(stdout.trim());
      } catch (e) {
        throw new Error(`Invalid JSON received from StreamHG: ${stdout}`);
      }

      if (resJson.status !== 200 || !resJson.files || !resJson.files[0]?.filecode) {
        const fileStatus = resJson.files?.[0]?.status || '';
        const isLimit = fileStatus.includes('too many files today') || (resJson.msg && resJson.msg.includes('too many files'));
        const errDetail = isLimit
          ? 'STREAMHG_LIMIT_REACHED: too many files today (200/day limit exceeded)'
          : (fileStatus || resJson.msg || 'Missing filecode in response');
        const err = new Error(`StreamHG upload failed: ${errDetail}`);
        if (isLimit) {
          err.isDailyLimit = true;
        }
        throw err;
      }

      const fileInfo = resJson.files[0];
      const filecode = fileInfo.filecode;
      const embedUrl = `https://hgcloud.to/e/${filecode}`;

      console.log(`[StreamHG] Upload complete! File Code: ${filecode} | Embed: ${embedUrl}`);

      fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
        job_id: jobId,
        status: 'ready',
        provider: 'streamhg',
        gh_run_id: process.env.GITHUB_RUN_ID || undefined,
        filecode: filecode,
        videoId: filecode,
        embedUrl: embedUrl,
        filename: fileName,
        size: fileSize,
        audio_languages: process.env.AUDIO_LANGUAGES ? process.env.AUDIO_LANGUAGES.split(',').filter(Boolean) : [],
        subtitle_languages: process.env.SUBTITLE_LANGUAGES ? process.env.SUBTITLE_LANGUAGES.split(',').filter(Boolean) : []
      }));

      // Successfully finished upload
      return;

    } catch (err) {
      lastError = err;
      console.warn(`[StreamHG] Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${err.message}`);

      const isDailyLimit = err.isDailyLimit ||
        err.message.includes('STREAMHG_LIMIT_REACHED') ||
        err.message.includes('too many files today');

      if (isDailyLimit) {
        console.error('[StreamHG] ⛔ Daily upload limit reached on this API key (200/day limit). Aborting upload retries immediately.');
        break;
      }

      if (attempt < MAX_ATTEMPTS) {
        console.log('[StreamHG] Waiting 5 seconds before retrying with a fresh upload server...');
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }

  // If all attempts failed
  console.error('[StreamHG] All upload attempts failed:', lastError?.message);
  fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
    job_id: jobId,
    status: 'error',
    provider: 'streamhg',
    error: lastError?.message || 'Upload failed after retries'
  }));
  process.exit(1);
}

upload();

