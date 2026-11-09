# ApexStream Public Ingestion Worker

Public GitHub Actions runner repository for ApexStream autonomous video ingestion, processing, and CDN streaming backups.

## Workflows Included
- `ingest-seek-movie-batch.yml`: Batched movie downloading, FFmpeg probing, metadata extraction, StreamHG / HF uploads.
- `ingest-seek-season.yml`: Batched series season downloading, transcoding, and provider indexing.
- `ingest-seek.yml`: Single title ingestion runner.

## Security
- Sensitive tokens are masked using GitHub Actions `::add-mask::` to prevent credentials leaking into public run logs.
- Can be supplied via workflow inputs from your API server or stored in Repository Secrets.
