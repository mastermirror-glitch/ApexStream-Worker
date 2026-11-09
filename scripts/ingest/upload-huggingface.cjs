/**
 * Standalone Hugging Face Dataset Uploader for GitHub Actions Runner
 * Uses official huggingface_hub commit endpoint to upload pristine video files
 * directly to a Hugging Face Dataset repository preserving the exact folder hierarchy:
 *   apexstream/movies/{Title}.{Year}/{Filename}
 *   apexstream/series/{Title}.{Year}/Season {S}/{Filename}
 */
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

async function uploadToHuggingFace() {
  const filePath   = process.env.FILE_PATH;
  const hfToken    = process.env.HF_TOKEN;
  const hfRepo     = process.env.HF_REPO;
  const targetPath = process.env.HF_TARGET_PATH;
  const jobId      = process.env.JOB_ID;

  if (!hfToken || !hfRepo || !targetPath) {
    console.log('[HuggingFace] HF_TOKEN, HF_REPO, or HF_TARGET_PATH not set. Skipping Hugging Face backup.');
    return null;
  }

  if (!filePath || !fs.existsSync(filePath)) {
    console.error(`[HuggingFace] Error: File does not exist: ${filePath}`);
    return null;
  }

  const pyScript = path.join(__dirname, 'upload_hf.py');
  if (!fs.existsSync(pyScript)) {
    console.error(`[HuggingFace] Error: Helper script not found: ${pyScript}`);
    return null;
  }

  // Detect python binary (python3 on linux runners, python on windows)
  const pythonBin = process.platform === 'win32' ? 'python' : 'python3';

  console.log(`[HuggingFace] Executing Python uploader via ${pythonBin}...`);

  return new Promise((resolve, reject) => {
    const proc = spawn(pythonBin, [pyScript], {
      env: {
        ...process.env,
        FILE_PATH: filePath,
        HF_TOKEN: hfToken,
        HF_REPO: hfRepo,
        HF_TARGET_PATH: targetPath,
        JOB_ID: jobId || '',
      },
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let resultJson = null;

    proc.stdout.on('data', chunk => {
      const text = chunk.toString();
      process.stdout.write(text);

      const lines = text.split('\n');
      for (const line of lines) {
        if (line.startsWith('[HuggingFace-Result]')) {
          try {
            resultJson = JSON.parse(line.replace('[HuggingFace-Result]', '').trim());
          } catch (_) {}
        }
      }
    });

    proc.stderr.on('data', chunk => {
      process.stderr.write(chunk.toString());
    });

    proc.on('close', code => {
      if (code === 0) {
        if (!resultJson && jobId) {
          const resultFile = `hf-result-${jobId}.json`;
          if (fs.existsSync(resultFile)) {
            try {
              resultJson = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
            } catch (_) {}
          }
        }
        resolve(resultJson);
      } else {
        reject(new Error(`Python HuggingFace uploader exited with code ${code}`));
      }
    });

    proc.on('error', err => {
      // If python3 is not found, fallback to python
      if (err.code === 'ENOENT' && pythonBin === 'python3') {
        console.warn('[HuggingFace] python3 not found, falling back to python...');
        const fallbackProc = spawn('python', [pyScript], {
          env: {
            ...process.env,
            FILE_PATH: filePath,
            HF_TOKEN: hfToken,
            HF_REPO: hfRepo,
            HF_TARGET_PATH: targetPath,
            JOB_ID: jobId || '',
          },
          stdio: 'inherit'
        });
        fallbackProc.on('close', c => c === 0 ? resolve(null) : reject(new Error(`Exit code ${c}`)));
        fallbackProc.on('error', reject);
      } else {
        reject(err);
      }
    });
  });
}

if (require.main === module) {
  uploadToHuggingFace().catch(err => {
    console.error('[HuggingFace] Fatal error:', err.message);
    process.exit(1);
  });
}

module.exports = { uploadToHuggingFace };
