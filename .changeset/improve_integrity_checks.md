---
default: patch
---

# Improve integrity check throughput by replacing batch-and-wait with a worker pool and adding a per-host timeout to prevent slow hosts from blocking progress.
