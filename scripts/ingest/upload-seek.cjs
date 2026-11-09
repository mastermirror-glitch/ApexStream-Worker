/**
 * Standalone SeekStreaming TUS Uploader for GitHub Actions Runner
 * Zero-dependency (uses native https module)
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const { Buffer } = require('buffer');

const CHUNK_SIZE = 10485760; // 10MB chunks for granular progress tracking

const RAW_CALLBACK = process.env.CALLBACK_URL || '';
const CALLBACK_URL = RAW_CALLBACK.replace(/\/api\/.*$/, '').replace(/\/+$/, '');
const INTERNAL_TOKEN = process.env.INTERNAL_API_TOKEN || process.env.INTERNAL_TOKEN || '';

async function reportProgress(jobId, status, progressPct) {
  if (!CALLBACK_URL || !INTERNAL_TOKEN) return;
  const payload = JSON.stringify({ job_id: jobId, status, progress_pct: progressPct });
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

async function upload() {
  const filePath = process.env.FILE_PATH;
  const cookies = process.env.SEEK_COOKIES;
  const jobId = process.env.JOB_ID;
  const folderId = process.env.SEEK_FOLDER_ID || '';

  if (!filePath || !cookies) {
    console.error('Missing FILE_PATH or SEEK_COOKIES');
    process.exit(1);
  }

  console.log(`[Ingest] Starting SeekStreaming upload for ${path.basename(filePath)}...`);

  try {
    // 1. Get Upload Info
    const info = await request('GET', 'https://seekstreaming.com/api/v1/video/upload', {
      'Accept': 'application/json',
      'Cookie': cookies
    });

    const { tusUrl, accessToken } = JSON.parse(info.body);
    if (!tusUrl || !accessToken) throw new Error('Invalid upload info response');

    const stats = fs.statSync(filePath);
    const fileSize = stats.size;
    const fileName = path.basename(filePath);

    // 2. Create TUS Resource
    const metadataArr = [
      `filename ${Buffer.from(fileName).toString('base64')}`,
      `filetype ${Buffer.from('video/mp4').toString('base64')}`,
      `accessToken ${Buffer.from(accessToken).toString('base64')}`
    ];
    
    if (folderId) {
      metadataArr.push(`folderId ${Buffer.from(folderId).toString('base64')}`);
    }

    const metadata = metadataArr.join(',');

    const createRes = await request('POST', tusUrl, {
      'Tus-Resumable': '1.0.0',
      'Upload-Length': fileSize.toString(),
      'Upload-Metadata': metadata
    });

    if (createRes.statusCode !== 201) throw new Error(`Failed to create TUS resource: ${createRes.statusCode}`);
    
    const location = createRes.headers['location'];
    if (!location) throw new Error('No Location header in TUS response');
    
    const uploadUrl = new URL(location, tusUrl).toString();
    console.log(`[Ingest] TUS Resource created: ${uploadUrl}`);

    // 3. Upload Chunks
    const fd = fs.openSync(filePath, 'r');
    let offset = 0;
    let lastBody = null;

    try {
      while (offset < fileSize) {
        const remaining = fileSize - offset;
        const currentChunkSize = Math.min(CHUNK_SIZE, remaining);
        const buffer = Buffer.alloc(currentChunkSize);
        fs.readSync(fd, buffer, 0, currentChunkSize, offset);

        const patchRes = await request('PATCH', uploadUrl, {
          'Tus-Resumable': '1.0.0',
          'Upload-Offset': offset.toString(),
          'Content-Type': 'application/offset+octet-stream'
        }, buffer);

        if (patchRes.statusCode !== 204 && patchRes.statusCode !== 200) {
          throw new Error(`Chunk upload failed at offset ${offset}: ${patchRes.statusCode}`);
        }

        offset += currentChunkSize;
        lastBody = patchRes.body;
        const pct = Math.floor((offset / fileSize) * 100);
        console.log(`[Ingest] Progress: ${pct}%`);
        await reportProgress(jobId, 'uploading', pct).catch(() => {});
      }
    } finally {
      fs.closeSync(fd);
    }

    // 4. Save result
    const result = JSON.parse(lastBody);
    if (result.videoId) {
      console.log(`[Ingest] Upload complete! Video ID: ${result.videoId}`);
      fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
        job_id: jobId,
        status: 'ready',
        videoId: result.videoId,
        filename: fileName,
        size: fileSize,
        audio_languages: process.env.AUDIO_LANGUAGES ? process.env.AUDIO_LANGUAGES.split(',').filter(Boolean) : [],
        subtitle_languages: process.env.SUBTITLE_LANGUAGES ? process.env.SUBTITLE_LANGUAGES.split(',').filter(Boolean) : []
      }));
    } else {
      throw new Error('Video ID missing in final response');
    }

  } catch (err) {
    console.error('[Ingest] Upload failed:', err.message);
    fs.writeFileSync(`ingest-result-${jobId}.json`, JSON.stringify({
      job_id: jobId,
      status: 'error',
      error: err.message
    }));
    process.exit(1);
  }
}

function request(method, url, headers, body = null) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      method,
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      headers: {
        'User-Agent': 'ApexStream-Ingest/2.0',
        ...headers
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: data }));
    });

    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

upload();
