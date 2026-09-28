# Rendering throughput and generated audio retention

Product rendering uses four routine workers by default, two reserved on-demand
workers, and one ordered CAP worker. Set `services.go.product_render.workers`
to change routine concurrency (1-32, applied on restart). Configuration snapshots
are immutable while jobs use them. Each work queue holds at most 64 waiting jobs;
routine/on-demand overflow returns an explicit failure so callers can retry.
CAP updates and cancellations share one ordered lane with archive maintenance.

CAP archive compression and decompression reuse one concurrency-safe Zstandard
workspace per direction. Compression level, archive format, and SHA-256 integrity
metadata remain unchanged. Creating a compressor for every feed/archive row caused
large allocations and garbage collection during multi-feed alert bursts.

In the September 2026 archive microbenchmark, a roughly 10 KiB XML document went
from 68 MB allocated and 4 ms per encode to 11 KiB and 12 microseconds after codec
initialization. This measures archive encoding, not complete end-to-end playout.

## Generated audio lifecycle

On routine playout completion, the playlist service checks that feed's generated
audio directory at most once per minute. It keeps:

- Audio referenced by the current item or queued items.
- Files less than five minutes old, protecting recently completed preparations.
- The newest generated WAV for each product/language, while it is within the
  existing 30-minute startup fallback window.

Older superseded generated WAVs are removed. Names must match the generated
product filename format; operator filenames, subdirectories, symlinks, and segment
scratch files are excluded. Existing daemon cleanup remains the fallback for
abandoned files and feeds that are no longer completing products.

On alert playout completion, a scan at most once per minute removes generated PCM
only for manifests marked `played` with a valid completion timestamp older than
15 minutes. The PCM must also be older than 15 minutes, protecting a replacement
written for a subsequent relay. Queued/playing alerts, manifests, and CAP archive
records remain intact.

Failed playlist state writes now remove their temporary file instead of leaving
one file behind on every tick during disk-full conditions.

## Footprint accounting

Generated audio retention is not a hard cap on the whole portable directory.
Voice models, binaries, location catalogs, deployment backups, logs, and retained
CAP history have separate lifecycles. Active and queued audio must not be deleted
to enforce a disk cap. Account for those fixed and historical costs before setting
a total deployment size target.
