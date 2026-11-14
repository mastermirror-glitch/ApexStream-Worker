/**
 * movie-batch-orchestrator.cjs
 * Pipeline: Download(5 parallel) → Process(1 serial) → Upload(5 parallel)
 * 
 * Disk Management: Before each download, the disk guard checks if enough
 * space is available. If not, it waits (polling every 5s) until upload
 * completions free up space. This makes it safe for large batches (10+ movies).
 * 
 * Movies flow through stages independently — stages overlap in time.
 */
const { execSync, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const https = require('https');
const { Buffer } = require('buffer');

const MOVIES_JSON    = process.env.MOVIES_JSON;
const SEEK_COOKIES   = process.env.SEEK_COOKIES;
const STREAMHG_KEY   = process.env.STREAMHG_KEY || process.env.STREAMHG_API_KEY || '';
const UPLOAD_PROVIDER = (process.env.UPLOAD_PROVIDER || (STREAMHG_KEY ? 'streamhg' : 'seekstreaming')).toLowerCase();
const RAW_CALLBACK   = process.env.CALLBACK_URL || '';
const CALLBACK_URL   = RAW_CALLBACK.replace(/\/api\/.*$/, '').replace(/\/+$/, '');
const SITE_NAME      = process.env.SITE_NAME || 'ApexStreaming.org';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || '';

// ── Disk Management Constants ────────────────────────────────────────────────
// Safety headroom: keep 3GB free at all times for OS + processing overhead.
const DISK_SAFETY_HEADROOM_MB = 3 * 1024;
// Overhead multiplier: processing can temporarily double a file's footprint
// (source + output exist simultaneously during transcode).
const DISK_OVERHEAD_MULTIPLIER = 2.2;
// Poll every 5 seconds when waiting for disk space to be freed
const DISK_POLL_INTERVAL_MS = 5000;

// ── Non-blocking spawn helper ────────────────────────────────────────────────
function spawnAsync(cmd, args, opts = {}, onProgress = null) {
  return new Promise((resolve, reject) => {
    // If we want progress, we need to pipe stdout/stderr
    const stdioOpt = onProgress ? ['ignore', 'pipe', 'pipe'] : 'inherit';
    const proc = spawn(cmd, args, { stdio: stdioOpt, shell: false, ...opts });

    if (onProgress) {
      let lastReported = -1;
      const handleData = (data) => {
        const text = data.toString();
        // Match yt-dlp: "[download]  45.3%"
        let match = text.match(/\[download\]\s+([\d\.]+)\%/);
        // Match aria2c: "(45%)"
        if (!match) match = text.match(/\(([\d\.]+)\%\)/);
        
        if (match) {
          const pct = Math.floor(parseFloat(match[1]));
          if (pct >= lastReported + 5 || pct === 100) {
            lastReported = pct;
            onProgress(pct);
          }
        }
        // Still pipe to stdout so we can see it in GitHub Actions
        process.stdout.write(data);
      };
      proc.stdout.on('data', handleData);
      proc.stderr.on('data', handleData);
    }

    proc.on('close', code => {
      if (code === 0) resolve(code);
      else reject(new Error(`${cmd} exited with code ${code}`));
    });
    proc.on('error', reject);
  });
}

// ── Progress Reporting ───────────────────────────────────────────────────────
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
          hostname: u.hostname, path: u.pathname, method: 'POST',
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

      if (statusCode >= 200 && statusCode < 300) return; // Success
      if (statusCode === 404 || statusCode >= 500) {
        console.error(`[Prog API] Status ${statusCode}. Retrying ${i + 1}/5...`);
      } else {
        return; // Unhandled status, stop retrying
      }
    } catch (e) {
      console.error(`[Prog API] Error: ${e.message}. Retrying ${i + 1}/5...`);
    }
    if (i < 5) await new Promise(r => setTimeout(r, 5000));
  }
}

// ── Concurrency Pool ─────────────────────────────────────────────────────────
class Pool {
  constructor(concurrency) {
    this.concurrency = concurrency;
    this.active = 0;
    this.queue = [];
  }
  add(fn) {
    return new Promise((resolve, reject) => {
      const run = () => {
        this.active++;
        Promise.resolve().then(() => fn()).then(resolve, reject).finally(() => {
          this.active--;
          if (this.queue.length > 0 && this.active < this.concurrency) this.queue.shift()();
        });
      };
      this.active < this.concurrency ? run() : this.queue.push(run);
    });
  }
}

// ── Disk Guard ───────────────────────────────────────────────────────────────
/**
 * Returns available disk space in MB on the current working directory.
 */
function getFreeDiskMB() {
  try {
    // Works on Linux (GitHub Actions ubuntu-latest)
    const out = execSync("df -BM . | tail -1 | awk '{print $4}'", { encoding: 'utf8' }).trim();
    return parseInt(out.replace('M', ''), 10);
  } catch {
    // Fallback: assume unlimited (should not happen on standard Linux)
    return 999999;
  }
}

/**
 * Waits until sufficient disk space is available for a file of `estimatedMB`.
 * Logs a warning and polls every 5 seconds if disk is tight.
 * The required space includes the OVERHEAD_MULTIPLIER to account for the
 * simultaneous existence of the source file and the processed output.
 */
async function waitForDiskSpace(label, estimatedMB) {
  const requiredMB = Math.ceil(estimatedMB * DISK_OVERHEAD_MULTIPLIER) + DISK_SAFETY_HEADROOM_MB;
  let freeMB = getFreeDiskMB();

  if (freeMB >= requiredMB) {
    console.log(`[DISK] ✓ ${label}: Need ${requiredMB}MB, have ${freeMB}MB free. Proceeding.`);
    return;
  }

  console.warn(`[DISK] ⏳ ${label}: Need ${requiredMB}MB but only ${freeMB}MB free. Waiting for uploads to free space...`);
  while (freeMB < requiredMB) {
    await new Promise(r => setTimeout(r, DISK_POLL_INTERVAL_MS));
    freeMB = getFreeDiskMB();
    console.log(`[DISK] ⏳ ${label}: Polling... ${freeMB}MB free (need ${requiredMB}MB)`);
  }
  console.log(`[DISK] ✓ ${label}: Space acquired (${freeMB}MB free). Starting download.`);
}

// ── Stage 1: Download ────────────────────────────────────────────────────────
async function downloadMovie(movie) {
  const label = `[${movie.job_id.substring(0, 8)}] ${movie.base_title}`;
  console.log(`\n[DL] ${label} → ${movie.filename}`);

  try {
    // Disk check before we start downloading
    await waitForDiskSpace(label, movie.estimated_size_mb || 2500);

    const onProgress = (pct) => reportProgress(movie.job_id, 'downloading', pct).catch(() => {});
    await reportProgress(movie.job_id, 'downloading', 0).catch(() => {});

    const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36';

    const validateDownloadedFile = () => {
      if (!fs.existsSync(movie.filename)) throw new Error(`Output file ${movie.filename} not found`);
      const sz = fs.statSync(movie.filename).size;
      if (sz < 5 * 1024 * 1024) {
        let snippet = '';
        try { snippet = fs.readFileSync(movie.filename, 'utf8').slice(0, 300); } catch (_) {}
        try { fs.unlinkSync(movie.filename); } catch (_) {}
        const isHtml = snippet.includes('<html') || snippet.includes('<!DOCTYPE') || snippet.includes('<head');
        throw new Error(`Download invalid (${(sz / 1024).toFixed(1)} KB${isHtml ? ' HTML error/404 page' : ''})`);
      }
    };

    const isGoogle = (u) => Boolean(u && (u.includes('googleusercontent.com') || u.includes('drive.google.com')));

    const runAria = async (targetUrl, singleConnection = false) => {
      const connArgs = singleConnection ? ['-s', '1', '-x', '1'] : ['-x', '16', '-s', '16', '-k', '1M'];
      await spawnAsync('aria2c', [
        ...connArgs,
        '--user-agent', ua,
        '--allow-overwrite=true',
        '--auto-file-renaming=false',
        '-o', movie.filename,
        targetUrl
      ], {}, onProgress);
      validateDownloadedFile();
    };

    const runYtDlp = async (targetUrl, useAria = true) => {
      const args = ['--user-agent', ua, '--no-playlist'];
      if (useAria) {
        args.push('--external-downloader', 'aria2c');
        args.push('--external-downloader-args', isGoogle(targetUrl) ? '-s 1 -x 1' : '-x 16 -s 16 -k 1M');
      }
      args.push(targetUrl, '-o', movie.filename);
      await spawnAsync('yt-dlp', args, {}, onProgress);
      validateDownloadedFile();
    };

    try {
      if (isGoogle(movie.download_url)) {
        console.log(`[DL] ${label}: Google CDN detected, using single-stream aria2c (-s 1 -x 1)...`);
        await runAria(movie.download_url, true);
      } else {
        await runYtDlp(movie.download_url, true);
      }
    } catch (e1) {
      console.log(`[DL] ${label}: Attempt 1 failed (${e1.message}), trying direct single-stream aria2c...`);
      try {
        // Direct single-stream aria2c download (bypasses byte-range multi-chunking & webpage extractors)
        await runAria(movie.download_url, true);
      } catch (e2) {
        console.log(`[DL] ${label}: direct aria2c failed (${e2.message}), trying native yt-dlp...`);
        try {
          await runYtDlp(movie.download_url, false);
        } catch (e3) {
          if (movie.fallback_url) {
            console.log(`[DL] ${label}: primary link failed, trying fallback_url...`);
            try {
              if (isGoogle(movie.fallback_url)) {
                await runAria(movie.fallback_url, true);
              } else {
                await runAria(movie.fallback_url, false);
              }
            } catch (e4) {
              await runYtDlp(movie.fallback_url, false);
            }
          } else {
            throw e3;
          }
        }
      }
    }

    const mb = (fs.statSync(movie.filename).size / 1024 / 1024).toFixed(0);
    console.log(`[DL] ✓ ${label}: ${mb}MB`);
    await reportProgress(movie.job_id, 'downloading', 100).catch(() => {});
    return { ...movie, downloadOk: true };
  } catch (err) {
    console.error(`[DL] ✗ ${label}: ${err.message}`);
    return { ...movie, downloadOk: false, error: err.message };
  }
}

// ── Stage 2: Process ─────────────────────────────────────────────────────────
function processMovie(movie) {
  const label = `[${movie.job_id.substring(0, 8)}] ${movie.base_title}`;
  if (!movie.downloadOk) return { ...movie, processOk: false };
  console.log(`\n[PROC] ${label} → ${movie.filename}`);

  // Report processing started (indeterminate progress)
  reportProgress(movie.job_id, 'processing', 0).catch(() => {});

  try {
    const out = execSync('node scripts/ingest/process-metadata.cjs', {
      env: {
        ...process.env,
        INPUT_FILE:     movie.filename,
        BASE_TITLE:     movie.base_title,
        YEAR:           movie.year || '',
        SITE_NAME,
        TARGET_QUALITY: movie.quality || '',
        SEASON_NUMBER:  '',
        EPISODE_NUMBER: '',
      },
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'inherit'],
    });

    const matchFile  = out.match(/Final processed file: (.+)/);
    const matchAudio = out.match(/Audio languages: (.*)/);
    const matchSubs  = out.match(/Subtitle languages: (.*)/);

    const processedFile     = matchFile  ? matchFile[1].trim()  : null;
    const audioLanguages    = matchAudio ? matchAudio[1].trim().split(',').filter(Boolean) : [];
    const subtitleLanguages = matchSubs  ? matchSubs[1].trim().split(',').filter(Boolean) : [];

    if (!processedFile || !fs.existsSync(processedFile)) throw new Error('No output file from processor');

    // ── DISK RECLAIM: Delete source immediately after successful transcode ──
    if (processedFile !== movie.filename && fs.existsSync(movie.filename)) {
      const srcMB = (fs.statSync(movie.filename).size / 1024 / 1024).toFixed(0);
      fs.unlinkSync(movie.filename);
      console.log(`[PROC] ♻ Deleted source ${movie.filename} (freed ~${srcMB}MB)`);
    }

    console.log(`[PROC] ✓ ${label}: ${processedFile} | audio=${audioLanguages.join(',')}`);
    reportProgress(movie.job_id, 'processing', 100).catch(() => {});
    return { ...movie, processedFile, audioLanguages, subtitleLanguages, processOk: true };
  } catch (err) {
    console.error(`[PROC] ✗ ${label}: ${err.message}`);
    // Reclaim source on failure too
    if (fs.existsSync(movie.filename)) { fs.unlinkSync(movie.filename); }
    return { ...movie, processOk: false, error: err.message };
  }
}

// ── Stage 3: Upload ──────────────────────────────────────────────────────────
async function uploadMovie(movie) {
  const label = `[${movie.job_id.substring(0, 8)}] ${movie.base_title}`;
  if (!movie.processOk) {
    await callback(movie.job_id, null, movie.error || 'Process failed', null);
    return;
  }
  console.log(`\n[UP] ${label} → ${movie.processedFile} (Provider: ${UPLOAD_PROVIDER})`);
  let hfData = null;
  try {
    const uploadScript = UPLOAD_PROVIDER === 'streamhg'
      ? 'scripts/ingest/upload-streamhg.cjs'
      : (UPLOAD_PROVIDER === 'lulustream'
        ? 'scripts/ingest/upload-lulustream.cjs'
        : 'scripts/ingest/upload-seek.cjs');

    const normalizedTitle = (movie.base_title || 'movie').replace(/\(\d{4}\)/g, '').trim().replace(/[<>:"/\\|?*']/g, '').replace(/\s+/g, '.') + (movie.year ? `.${movie.year}` : '');
    const hfTargetPath = `apexstream/movies/${normalizedTitle}/${path.basename(movie.processedFile)}`;

    // Parallel Dual Upload: StreamHG/LuluStream + Hugging Face
    const uploadTasks = [
      spawnAsync('node', [uploadScript], {
        env: {
          ...process.env,
          FILE_PATH: movie.processedFile,
          SEEK_COOKIES,
          STREAMHG_KEY,
          LULUSTREAM_KEY: STREAMHG_KEY,
          JOB_ID: movie.job_id,
          SEEK_FOLDER_ID: movie.folder_id || '',
          STREAMHG_FOLDER_ID: movie.folder_id || '',
          LULUSTREAM_FOLDER_ID: movie.folder_id || '',
          FILE_TITLE: path.basename(movie.processedFile),
        },
      })
    ];

    if (process.env.HF_TOKEN && process.env.HF_REPO && process.env.HF_BACKUP_ENABLED !== 'false') {
      uploadTasks.push(
        spawnAsync('node', ['scripts/ingest/upload-huggingface.cjs'], {
          env: {
            ...process.env,
            FILE_PATH: movie.processedFile,
            HF_TOKEN: process.env.HF_TOKEN,
            HF_REPO: process.env.HF_REPO,
            HF_TARGET_PATH: hfTargetPath,
            JOB_ID: movie.job_id,
          }
        }).catch(hfErr => {
          console.warn(`[HF Backup Warning] Hugging Face backup failed for ${movie.base_title}: ${hfErr.message}`);
          return null;
        })
      );
    }

    await Promise.all(uploadTasks);

    const resultFile = `ingest-result-${movie.job_id}.json`;
    if (!fs.existsSync(resultFile)) throw new Error('Upload did not write result file');
    const result = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
    fs.unlinkSync(resultFile);

    const hfResultFile = `hf-result-${movie.job_id}.json`;
    if (fs.existsSync(hfResultFile)) {
      try {
        hfData = JSON.parse(fs.readFileSync(hfResultFile, 'utf8'));
        fs.unlinkSync(hfResultFile);
      } catch (_) {}
    }

    // ── DISK RECLAIM: Delete processed file immediately after upload ──
    if (fs.existsSync(movie.processedFile)) {
      const mb = (fs.statSync(movie.processedFile).size / 1024 / 1024).toFixed(0);
      fs.unlinkSync(movie.processedFile);
      console.log(`[UP] ♻ Deleted processed file (freed ~${mb}MB)`);
    }

    if (result.status === 'error') throw new Error(result.error);

    console.log(`[UP] ✓ ${label}: videoId=${result.videoId}${hfData?.hf_url ? ' (HF Backup ✓)' : ''}`);
    await callback(movie.job_id, result.videoId, null, movie.processedFile, movie.audioLanguages || [], movie.subtitleLanguages || [], hfData);
  } catch (err) {
    let finalError = err.message;
    const resultFile = `ingest-result-${movie.job_id}.json`;
    if (fs.existsSync(resultFile)) {
      try {
        const result = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
        if (result.error) finalError = result.error;
        fs.unlinkSync(resultFile);
      } catch (_) {}
    }
    console.error(`[UP] ✗ ${label}: ${finalError}`);
    if (movie.processedFile && fs.existsSync(movie.processedFile)) fs.unlinkSync(movie.processedFile);
    await callback(movie.job_id, null, finalError, null, [], [], hfData);
  }
}

// ── Callback ─────────────────────────────────────────────────────────────────
async function callback(jobId, videoId, error, filename, audioLanguages = [], subtitleLanguages = [], hfData = null) {
  if (!CALLBACK_URL || !INTERNAL_TOKEN) return;
  const ghRunId = process.env.GITHUB_RUN_ID;
  const payload = videoId
    ? { job_id: jobId, status: 'ready', videoId, provider: UPLOAD_PROVIDER, filename: filename ? path.basename(filename) : '',
        audio_languages: audioLanguages, subtitle_languages: subtitleLanguages, gh_run_id: ghRunId || undefined,
        hf_url: hfData?.hf_url || undefined, hf_path: hfData?.hf_path || undefined, hf_repo: hfData?.hf_repo || undefined }
    : { job_id: jobId, status: 'error', provider: UPLOAD_PROVIDER, error, gh_run_id: ghRunId || undefined,
        hf_url: hfData?.hf_url || undefined, hf_path: hfData?.hf_path || undefined, hf_repo: hfData?.hf_repo || undefined };
  const body = JSON.stringify(payload);
  const url = `${CALLBACK_URL}/api/v1/internal/ingest/callback`;

  for (let i = 0; i <= 5; i++) {
    try {
      const statusCode = await new Promise((resolve, reject) => {
        const u = new URL(url);
        const req = https.request({
          hostname: u.hostname, path: u.pathname, method: 'POST',
          headers: {
            'Authorization': `Bearer ${INTERNAL_TOKEN}`,
            'Content-Type': 'application/json',
            'User-Agent': 'ApexStream-Internal-Worker/1.0',
            'Content-Length': Buffer.byteLength(body),
          }
        }, res => { res.resume(); resolve(res.statusCode); });
        req.on('error', reject);
        req.write(body);
        req.end();
      });

      if (statusCode >= 200 && statusCode < 300) {
        console.log(`[CB] job=${jobId.substring(0,8)} → ${statusCode}`);
        return; // Success
      }
      if (statusCode === 404 || statusCode >= 500) {
        console.error(`[CB] Status ${statusCode}. Retrying ${i + 1}/5...`);
      } else {
        console.error(`[CB] Unhandled status ${statusCode}. Stop retrying.`);
        return; // Unhandled status, stop retrying
      }
    } catch (e) {
      console.error(`[CB] Error: ${e.message}. Retrying ${i + 1}/5...`);
    }
    if (i < 5) await new Promise(r => setTimeout(r, 5000));
  }
}

// ── Main Pipeline ────────────────────────────────────────────────────────────
async function main() {
  if (!MOVIES_JSON) { console.error('[Orchestrator] MOVIES_JSON not set'); process.exit(1); }
  const movies = JSON.parse(MOVIES_JSON);

  const freeMB = getFreeDiskMB();
  console.log(`[Orchestrator] Starting batch: ${movies.length} movies | DL:5 PROC:1 UP:2 | Disk: ${freeMB}MB free`);

  const dlPool   = new Pool(5);
  const procPool = new Pool(1);
  const upPool   = new Pool(2);

  await Promise.all(
    movies.map(movie =>
      dlPool.add(() => downloadMovie(movie))
        .then(d  => procPool.add(() => processMovie(d)))
        .then(p  => upPool.add(() => uploadMovie(p)))
        .catch(e => console.error(`[Orchestrator] ${movie.base_title} fatal:`, e))
    )
  );

  const finalDiskMB = getFreeDiskMB();
  console.log(`\n[Orchestrator] Batch complete! ${movies.length} movies processed. Disk: ${finalDiskMB}MB free.`);
}

main().catch(err => { console.error('[Orchestrator] Fatal:', err); process.exit(1); });
