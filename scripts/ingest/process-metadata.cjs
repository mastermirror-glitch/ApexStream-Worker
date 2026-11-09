const { spawn, execSync } = require('child_process');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

/**
 * process-metadata.js
 * 
 * Implements the full sanitization logic from VideoProcessor.php in Node.js
 * for the GitHub Actions runner, with real-time FFmpeg progress reporting.
 */

const inputFile = process.env.INPUT_FILE;
const baseTitle = process.env.BASE_TITLE || '';
const year = process.env.YEAR || '';
const siteName = process.env.SITE_NAME || 'ApexStreaming.org';
const targetQuality = process.env.TARGET_QUALITY || '';
const seasonNumber = process.env.SEASON_NUMBER || '';
const episodeNumber = process.env.EPISODE_NUMBER || '';

const JOB_ID = process.env.JOB_ID || '';
const RAW_CALLBACK = process.env.CALLBACK_URL || '';
const CALLBACK_URL = RAW_CALLBACK.replace(/\/api\/.*$/, '').replace(/\/+$/, '');
const INTERNAL_TOKEN = process.env.INTERNAL_API_TOKEN || process.env.INTERNAL_TOKEN || '';
const GITHUB_RUN_ID = process.env.GITHUB_RUN_ID || '';

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
  console.log(`[Processor] Progress: ${status} -> ${pct}%`);

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
          'User-Agent': 'ApexStream-Processor/1.0',
          'Content-Length': Buffer.byteLength(payload),
        },
        timeout: 5000
      }, res => {
        res.resume();
        resolve(res.statusCode);
      });
      req.on('error', () => resolve(null));
      req.write(payload);
      req.end();
    });
  } catch (err) {}
}

if (!inputFile || !fs.existsSync(inputFile)) {
  console.error(`Input file not found: ${inputFile}`);
  process.exit(1);
}

// Derive standard quality tier from actual video height (source of truth)
function getResolutionQuality(width, height) {
  height = parseInt(height) || 0;
  width  = parseInt(width)  || 0;
  if (height >= 2160 || width >= 3840) return '4K';
  if (height >= 1440 || width >= 2560) return '1440p';
  if (height >= 1080 || width >= 1920) return '1080p';
  if (height >= 720  || width >= 1280) return '720p';
  if (height >= 480  || width >= 854)  return '480p';
  if (height >= 360  || width >= 640)  return '360p';
  if (height >= 240  || width >= 426)  return '240p';
  return '144p';
}

// Map ffprobe codec_name to common tag
function getCodecTag(codecName) {
  if (!codecName) return '';
  const c = codecName.toLowerCase();
  if (c === 'h264' || c === 'avc')  return 'x264';
  if (c === 'hevc' || c === 'h265') return 'x265';
  if (c === 'av1')                  return 'AV1';
  if (c === 'vp9')                  return 'VP9';
  if (c === 'mpeg4')                return 'XViD';
  return codecName.toUpperCase();
}

const ISO_LANGUAGES = require('./iso-languages.cjs');

function getLanguageName(lang) {
  const code = lang.toLowerCase();
  return ISO_LANGUAGES[code] || lang.toUpperCase();
}

async function run() {
  console.log(`[Processor] Analyzing: ${inputFile}`);

  if (!fs.existsSync(inputFile)) {
    throw new Error(`Input file "${inputFile}" does not exist`);
  }
  const fileStat = fs.statSync(inputFile);
  if (fileStat.size < 5 * 1024 * 1024) {
    let snippet = '';
    try { snippet = fs.readFileSync(inputFile, 'utf8').slice(0, 300); } catch (_) {}
    const isHtml = snippet.includes('<html') || snippet.includes('<!DOCTYPE') || snippet.includes('<head');
    throw new Error(`Media file is invalid/corrupted (${(fileStat.size / 1024).toFixed(1)} KB${isHtml ? ', received HTML error/404 page' : ''})`);
  }

  let totalDuration = 0;
  try {
    const probeOut = execSync(`ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputFile}"`, { encoding: 'utf-8' });
    totalDuration = parseFloat(probeOut.trim()) || 0;
    if (totalDuration > 0) {
      console.log(`[Processor] Media Duration: ${totalDuration.toFixed(1)}s`);
    }
  } catch (e) {
    console.warn(`[Processor] Could not probe duration: ${e.message}`);
  }

  await reportProgress(JOB_ID, 'processing', 0, true);

  // 1. Analyze Streams
  const probeData = JSON.parse(execSync(`ffprobe -v quiet -print_format json -show_streams "${inputFile}"`).toString());
  const videoStream = probeData.streams.find(s => s.codec_type === 'video');
  const audioStreams = probeData.streams.filter(s => s.codec_type === 'audio');
  const subtitleStreams = probeData.streams.filter(s => s.codec_type === 'subtitle');

  // ── Derive everything from actual stream metadata ──────────────────────────
  const quality   = getResolutionQuality(videoStream?.width, videoStream?.height);
  const codec     = getCodecTag(videoStream?.codec_name);
  const bitDepth  = (videoStream?.pix_fmt?.includes('10')) ? '10Bit' : '';

  // Audio languages — unique, drop 'und', cap at 3 + MDubs tag
  const allAudioLangs = [...new Set(
    audioStreams.map(s => (s.tags?.language || 'und').toLowerCase())
  )].filter(l => l !== 'und');
  const audioLangs   = allAudioLangs.slice(0, 3);
  const multiDub     = allAudioLangs.length > 3 ? 'MDubs' : '';

  // Subtitle languages — unique, deduplicated
  const allSubLangs = [...new Set(
    subtitleStreams.map(s => (s.tags?.language || 'und').toLowerCase())
  )].filter(l => l !== 'und');

  // Subtitle tag — ESubs (≤3 tracks), MSubs (>3 tracks), empty if none
  const subTag = subtitleStreams.length === 0 ? ''
               : subtitleStreams.length <= 3  ? 'ESubs' : 'MSubs';

  // Source type keyword from input filename
  const sourceKeyword = ['BRRip','WEB-DL','WEBRip','BluRay','HDRip','DVDRip','BDRip']
    .find(k => inputFile.toUpperCase().includes(k.toUpperCase())) || '';

  // 2. Build New Filename (SxxExx format)
  let namePart = baseTitle.replace(/\(\d{4}\)/g, '').trim().replace(/[<>:"/\\|?*]/g, '').replace(/\s+/g, '.');

  let episodePart = '';
  if (seasonNumber) {
    const s = seasonNumber.padStart(2, '0');
    const e = episodeNumber ? episodeNumber.padStart(2, '0') : '01';
    episodePart = `S${s}E${e}`;
  }

  const filenameParts = [
    namePart,
    episodePart,
    year,
    quality,          // derived from actual height — e.g. 720p, 1080p
    codec,            // derived from codec_name — e.g. x264, x265
    bitDepth,         // e.g. 10Bit if 10-bit pixel format
    sourceKeyword,    // e.g. WEB-DL if found in source filename
    ...audioLangs.map(l => l.toUpperCase()),  // top 3, uppercase for filename
    multiDub,         // 'MDubs' if > 3 audio tracks
    subTag,           // 'ESubs' (≤3 sub tracks) or 'MSubs' (>3)
    siteName
  ].filter(Boolean);

  const ext = path.extname(inputFile);
  const finalFilename = filenameParts.join('.') + ext;
  console.log(`[Processor] Target Filename: ${finalFilename}`);

  // 3. Clean Subtitles (Single-Pass Extraction)
  const cleanedSubs = [];
  
  if (subtitleStreams.length > 0) {
    console.log(`[Processor] Extracting ${subtitleStreams.length} subtitle tracks in one pass...`);
    const extractCmd = ['ffmpeg', '-y', '-i', `"${inputFile}"`];
    
    subtitleStreams.forEach((_, i) => {
      extractCmd.push('-map', `0:s:${i}`, '-c:s', 'srt', `"temp_sub_${i}.srt"`);
    });

    try {
      // One single read of the massive input file for all subtitles
      execSync(extractCmd.join(' '), { stdio: 'ignore' });
    } catch (e) {
      console.warn(`[Processor] Subtitle extraction warnings (some tracks might be empty or corrupted).`);
    }

    // Process extracted SRTs instantly in memory
    subtitleStreams.forEach((stream, i) => {
      const tempSrt = `temp_sub_${i}.srt`;
      const lang = stream.tags?.language || 'und';
      
      if (fs.existsSync(tempSrt)) {
        try {
          let content = fs.readFileSync(tempSrt, 'utf8').trim();
          if (!content) {
            fs.unlinkSync(tempSrt);
            return;
          }

          const blocks = content.split(/\r?\n\r?\n/);
          
          const cleanBlocks = blocks.filter(block => {
            if (!block.trim()) return false;
            const lower = block.toLowerCase();
            const hasPromo = /https?:\/\/|www\.|moviesmod|moviemod|hubflix|telegram|encoded|download|visit|subscribe|website|streaming|hdmp4|mkvcinemas|moviesverse|vegamovies/i.test(lower);
            return !hasPromo;
          });

          if (cleanBlocks.length > 0) {
            fs.writeFileSync(tempSrt, cleanBlocks.join('\n\n'));
            cleanedSubs.push({ path: tempSrt, index: i, lang });
          } else {
            fs.unlinkSync(tempSrt); // Delete if entirely promotional
          }
        } catch (err) {
          console.warn(`[Processor] Failed to clean ${tempSrt}: ${err.message}`);
        }
      }
    });
  }

  // 4. Final Re-mux and Metadata Injection
  const metadataTitle = `${baseTitle} ${year} [${quality}] [${siteName}]`.trim();
  const cmd = [
    'ffmpeg', '-i', `"${inputFile}"`,
    ...cleanedSubs.map(s => `-i "${s.path}"`),
    '-map', '0:v?', '-map', '0:a?',
    ...cleanedSubs.map((_, idx) => `-map ${idx + 1}:s`),
    '-c:v', 'copy', '-c:a', 'copy',
    '-c:s', (ext === '.mp4' ? 'mov_text' : 'srt'),
    '-map_metadata', '-1',
    '-metadata', `title="${metadataTitle}"`,
    '-metadata', `comment="${metadataTitle} - For watching more visit ${siteName}"`
  ];

  // Audio track metadata
  audioStreams.forEach((s, i) => {
    const langCode = s.tags?.language || 'und';
    const langName = getLanguageName(langCode);
    cmd.push(
      `-metadata:s:a:${i}`, `title="[${langName}] - ${siteName}"`,
      `-metadata:s:a:${i}`, `language="${langCode}"`
    );
  });

  // Subtitle track metadata
  cleanedSubs.forEach((s, i) => {
    const langCode = s.lang || 'und';
    const langName = getLanguageName(langCode);
    cmd.push(
      `-metadata:s:s:${i}`, `title="[${langName}] - ${siteName}"`,
      `-metadata:s:s:${i}`, `language="${langCode}"`
    );
  });


  const statsInput = fs.statSync(inputFile);
  console.log(`[Processor] Source Size: ${(statsInput.size / 1024 / 1024).toFixed(2)} MB`);

  cmd.push('-progress', 'pipe:1');
  cmd.push('-y', `"${finalFilename}"`);

  console.log(`[Processor] Executing FFmpeg with real-time progress...`);
  await new Promise((resolve, reject) => {
    const ffProc = spawn(cmd.join(' '), { shell: true });
    let buffer = '';

    ffProc.stdout.on('data', chunk => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep last incomplete line

      for (const line of lines) {
        const trimmed = line.trim();
        // Parse out_time_us=45000000
        const mUs = trimmed.match(/^out_time_us=(\d+)/);
        if (mUs && totalDuration > 0) {
          const outSec = parseInt(mUs[1], 10) / 1000000;
          const pct = Math.min(99, Math.floor((outSec / totalDuration) * 100));
          reportProgress(JOB_ID, 'processing', pct);
        } else if (trimmed.startsWith('progress=end')) {
          reportProgress(JOB_ID, 'processing', 100, true);
        }
      }
    });

    ffProc.stderr.on('data', chunk => {
      process.stderr.write(chunk);
    });

    ffProc.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with status ${code}`));
    });

    ffProc.on('error', reject);
  });

  await reportProgress(JOB_ID, 'processing', 100, true);

  const statsOutput = fs.statSync(finalFilename);
  console.log(`[Processor] Output Size: ${(statsOutput.size / 1024 / 1024).toFixed(2)} MB`);

  // Clean up
  cleanedSubs.forEach(s => fs.unlinkSync(s.path));
  if (finalFilename !== inputFile) {
    fs.unlinkSync(inputFile);
  }

  // Export the final filename + language arrays for the next step (Upload/Callback)
  const outputData = [
    `final_filename=${finalFilename}`,
    `audio_languages=${allAudioLangs.join(',')}`,
    `subtitle_languages=${allSubLangs.join(',')}`,
  ];
  if (process.env.GITHUB_OUTPUT) {
    fs.appendFileSync(process.env.GITHUB_OUTPUT, outputData.join('\n') + '\n');
  }
  console.log(`Final processed file: ${finalFilename}`);
  console.log(`Audio languages: ${allAudioLangs.join(',')}`);
  console.log(`Subtitle languages: ${allSubLangs.join(',')}`);
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
