# Native Pouch container foundation

This is an experimental physical storage boundary, not a Pouch backend or a
replacement public API. STATE keys replace immutable values; COLLECTION keys
reject overwrite of a live binding. Deleted keys can be rebound: permanent
position allocation, tombstones and retry authority belong to a future adapter.
Neither implements public leases, JSON validation, codecs, indexes, XA or
receipts. Comparisons must identify these differences rather than claim feature
parity. The comparison adapter separately applies the existing collection JSON
validator before publishing collection-shaped values.

One exclusive Linux `flock` protects the container lifetime. Independent threads
reserve private extents under a short mutex; producer callbacks run outside it.
A second open, including another process, fails. After the exclusive owner
exits, another process can recover and open normally. Shared-root coordination
and process-shared reader pins are not implemented. Fork before open or exec
before using the library in a child. Stop concurrent calls and close all readers
and writers before closing their space.

The initial capacity defaults to 10 MiB with real Linux `fallocate`. Growth is
explicit in 1 MiB multiples; fixed-capacity failure is the default. No sparse or
zero-filling fallback exists. Two 4 KiB root pages, a protected 128 KiB metadata
allocation region and a 128 KiB publication journal occupy 264 KiB. The typed
copy-on-write directory and packed descriptor pages start in that metadata
region and can allocate further pages from the common pool. The global 64 KiB
checkpoint ceiling is removed. A single publication descriptor still fits the
128 KiB journal; extent manifests for larger individual streams remain required
before large-capacity testing. No 250 MiB or 100 GB test is claimed.

Foreground admission preserves 16 KiB of payload capacity for maintenance plus
a conservative allowance for the next complete live metadata directory and its
packed records. Actual checkpoints copy only changed descriptors and tree paths.
The allowance uses the directory's minimum occupancy and current logical
metadata size; it grows with live metadata rather than preallocated capacity.
Capacity errors preserve prior publications and leave checkpoint/deletion work
possible. These constants and the experimental format apply to fresh fixtures.

Private payloads flow through a 16 KiB buffer into page-aligned runs. Allocation
uses 4 KiB physical pages, matching the measurement host's filesystem blocks;
its device reports 512-byte sectors. Physical alignment and packing small
logical records are separate decisions. The current allocator reserves at least
one page per nonempty payload unit and writes only its actual bytes. Packing
small values into shared pages remains future work.
Allocation uses sorted free intervals, split/merged on demand, and metadata
grows with actual extents and participation, not reserved capacity. Each run
records its offset, byte length and CRC32. References bind a random container
identity and a monotonically allocated incarnation; keys bind type, owner and
numeric key. Each open durably reserves a range of 2^32 incarnation numbers
before admission, so a
returned NoSync reference cannot alias a later value even if its publication is
lost. Reserving ranges amortizes durability without a per-writer sync or entropy
syscall. Unused numbers are abandoned on restart; exhaustion fails explicitly.
The 80-byte root header checkpoints the incarnation limit alongside capacity,
sequence, directory offset/generation and container identity, with an independent
checksum. Further grants use 64-byte journal events. Payload reads
use a pinned extent vector and a bounded verification buffer, without consulting
the directory on every chunk. Hash buckets resolve keys. CRC32 detects accidental
damage and torn writes; this is not authenticated storage.

Publication appends a checksummed metadata event only after payload writes
complete. The directory mutex serializes this short append/install step. A
caller then leads synchronization of the ready sequence prefix, or waits for
the active leader. The directory and commit coordination mutexes are released
throughout the file sync: other producers, allocations, publications and read
admission can progress. Arrivals during a barrier form the next group, without
an artificial collection delay or a background worker. One writer still issues
one file sync per successful commit.

Durable success requires a completed file sync covering that caller's sequence.
Readers select the completed durable prefix: pending replacements and deletes
leave the prior committed value visible. In-memory version links and temporary
delete markers retain that view until checkpoint; they add no payload copies
and do not change the disk format. NoSync mode exposes complete publications
without waiting for a commit barrier. Read handles retain immutable pins.
Checkpoint holds the directory mutex, shares a separate sync I/O mutex with
commit barriers, and advances durability only after its structural barriers.
Commit completion never acquires the directory mutex, avoiding a lock cycle.
Failed publication I/O poisons the handle: no further operations can reuse space
or overwrite an uncertain journal suffix. Reopen reconciles the outcome.
Status counters describe in-memory accounting; reopen to reconcile durable state
after an I/O error.

Recovery selects the highest valid root, validates its bound directory and
packed descriptors, then replays the contiguous checksummed journal suffix.
Journal bodies are verified
with bounded buffers, allowing torn or reordered unsynced tails to be discarded.
Checkpoint bodies are verified when read; open does not scan them or unused
capacity. A valid newest root with a damaged checkpoint is an error, not grounds
to fall back into possibly reused extents. An invalid/torn root can fall back
only while the older root is still valid. Recovery stabilizes the selected root
and accepted journal, invalidates its sibling, and clears the rejected journal
suffix durably before admitting allocation. Arbitrary corruption of previously
durable journal bytes is outside the torn-tail recovery guarantee.

Reopening retains ownership of every accepted checkpoint and journal body,
including superseded values. The next replay still verifies older PUT bodies
before reaching later replacements or deletions. Only a durable checkpoint
retires that dependency and permits reuse; an opening sync alone cannot do so.

Retired payload runs remain unavailable until a checkpoint has synchronized
payloads and metadata, published the alternate root durably, and invalidated the
previous root durably. Only then can unpinned runs be reused. Active readers pin
old entries even after that barrier. This makes NoSync deletion/replacement safe:
the old durable publication cannot recover after its runs become writable.
Private abandoned reservations need no durable ownership record: recovery finds
only published extents, so their complement is free after its opening barrier.

Descriptors share immutable 4 KiB pages with checksummed identity, location and
generation headers. A descriptor may span pages; its checked span determines
ownership. An unused continuation pointer does not retain a page after the
spanning record is retired. Pages are reclaimed after their last current record
and old recoverable root are retired. Reader extent vectors retain payload pins
independently of descriptor-page lifetime. Packing fragmented metadata pages is
future maintenance work.

Growth reserves backing space first and publishes capacity through that same
checkpoint protocol. Uncommitted trailing space is ignored on open. Any failed
structural I/O poisons the handle. Allocation failure before root publication
reclaims private metadata pages and permits retry. Journal exhaustion uses an
incremental metadata checkpoint; no live payload is copied. Checkpoint/growth
serialize publication and allocation, but never wait on a producer callback or
on readers to finish.

The protected metadata region cannot be consumed by payloads. Relocating
compaction, metadata repacking, transforms,
multi-process coordination and full Pouch integration remain future work. The
foundation implements retirement/reuse without relocation and makes no claim
that all draft acceptance criteria are already satisfied.

See [measurements and reproduction](../../docs/pouch-space-experiment.md) for
the verified scope, observed regressions and commands.
