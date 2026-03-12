# CBO Calibration Results — Multi-Machine Comparison

**Date:** 2026-03-11
**Dataset:** N=50,000 docs, Indexes: a, b, c, (a,b), (a,-b), (c,a)
**Data distribution:** a=i%100 (1%), b=i%200 (0.5%), c=i%10 (10%), d=i%1000 (not indexed)

## Hardware

| Machine | CPU | Cores | RAM | Storage |
|---------|-----|-------|-----|---------|
| **Local** | AMD Ryzen 9 9950X | 32 | 84 GB | WD_BLACK SN850X NVMe 2TB |
| **Laptop** | Intel i7-8550U @ 1.80GHz | 8 | 16 GB | WDC PC SN720 NVMe 512GB |
| **PC** | AMD Ryzen 9 3900X | 24 | 32 GB | WDC WDS250G2B0A SATA SSD 250GB |
| **RPi 4** | Cortex-A72 @ 1.5GHz | 4 | 4 GB | SD card (mmcblk0) |

> RPi binary was cross-compiled (`GOOS=linux GOARCH=arm64`) since Pi has Go 1.25.5
> which conflicts with the fdatasync build tag.

## Median Times (µs)

| Query | Docs | Cost | Local | Laptop | PC | RPi | Plan |
|-------|------|------|-------|--------|----|-----|------|
| **Baselines** |
| fullscan-all | 50000 | 30000 | 4324 | 10728 | 10380 | 49621 | FullScan |
| fullscan-filter-d | 50 | 30000 | 4656 | 11874 | 10889 | 55623 | FullScan(filtered) |
| fullscan-sort-d | 50000 | 225121 | 24272 | 60903 | 54013 | 239413 | FullScan -> Sort |
| **Equality (indexed)** |
| eq-a (1%) | 500 | 1751 | 351 | 1034 | 1053 | 8318 | IndexScan(a) |
| eq-c (10%) | 5000 | 17501 | 1953 | 5624 | 5205 | 33232 | IndexScan(c) |
| eq-b (0.5%) | 250 | 876 | 78 | 198 | 181 | 780 | IndexScan(b) |
| **Range** |
| range-a (10%) | 5000 | 30000 | 4785 | 11878 | 11177 | 54719 | FullScan(filtered) |
| range-a (50%) | 25000 | 30000 | 5013 | 12572 | 11934 | 58953 | FullScan(filtered) |
| range-a (1%) | 500 | 30000 | 4714 | 11673 | 11019 | 56444 | FullScan(filtered) |
| **Sort (no filter)** |
| sort-a-asc | 50000 | 200000 | 33613 | 101458 | 100077 | 795496 | IndexScan(a) -> Fetch |
| sort-a-desc | 50000 | 200000 | 33457 | 102680 | 100314 | 797333 | IndexScan(a)(rev) -> Fetch |
| sort-ab | 50000 | 200000 | 34674 | 103517 | 98690 | 798202 | IndexScan(a,b) -> Fetch |
| sort-a-negb | 50000 | 200000 | 34060 | 103194 | 100032 | 796010 | IndexScan(a,-b) -> Fetch |
| sort-ca | 50000 | 200000 | 34536 | 107102 | 100220 | 796811 | IndexScan(c,a) -> Fetch |
| **Sort + Limit** |
| sort-a-lim10 | 10 | 40 | 7 | 28 | 16 | 139 | IndexScan -> Limit(10) |
| sort-a-lim100 | 100 | 400 | 29 | 120 | 70 | 400 | IndexScan -> Limit(100) |
| sort-a-lim1000 | 1000 | 4000 | 690 | 2046 | 2015 | 15516 | IndexScan -> Limit(1000) |
| sort-ab-lim10 | 10 | 40 | 7 | 18 | 16 | 90 | IndexScan(a,b) -> Limit(10) |
| sort-ca-lim10 | 10 | 40 | 8 | 29 | 15 | 110 | IndexScan(c,a) -> Limit(10) |
| **Filter + Sort (index covers both)** |
| eq-a+sort-b | 500 | 1751 | 363 | 1056 | 1073 | 8321 | IndexScan(a,b) |
| eq-a+sort-b-desc | 500 | 1751 | 362 | 1062 | 1071 | 8468 | IndexScan(a,-b) |
| eq-c+sort-a | 5000 | 17501 | 3672 | 10889 | 10629 | 83911 | IndexScan(c,a) |
| **Filter + Sort (needs in-mem sort)** |
| eq-a+sort-c | 500 | 1759 | 559 | 1536 | 1489 | 10690 | IndexScan -> Sort |
| eq-a+sort-d | 500 | 1759 | 466 | 1201 | 1159 | 7213 | IndexScan -> Sort |
| **Filter + Sort + Limit** |
| eq-a+sort-b+lim10 | 10 | 1751 | 9 | 20 | 21 | 128 | IndexScan(a,b) -> Limit(10) |
| eq-a+sort-b+lim100 | 100 | 1751 | 34 | 87 | 78 | 432 | IndexScan(a,b) -> Limit(100) |
| eq-c+sort-a+lim10 | 10 | 200 | 9 | 21 | 18 | 326 | IndexScan(c,a) -> Limit(10) |
| eq-c+sort-a+lim100 | 100 | 2000 | 35 | 87 | 79 | 364 | IndexScan(c,a) -> Limit(100) |
| **ID sort (no filter)** |
| sort-id-asc | 50000 | 30000 | 4263 | 10615 | 10329 | 49881 | FullScan |
| sort-id-desc | 50000 | 30000 | 4241 | 10571 | 10309 | 50637 | FullScan(reverse) |
| sort-id-lim10 | 10 | 30000 | 5 | 12 | 10 | 63 | FullScan -> Limit(10) |
| sort-id-desc-lim10 | 10 | 30000 | 4 | 12 | 10 | 66 | FullScan(rev) -> Limit(10) |
| **ID sort + filter (NEW — was broken)** |
| eq-a+sort-id-asc | 500 | 1759 | 555 | 1509 | 1474 | 10392 | IndexScan -> Sort |
| eq-a+sort-id-desc | 500 | 1759 | 553 | 1529 | 1467 | 10433 | IndexScan -> Sort |
| eq-a+sort-id-asc-lim10 | 10 | 1759 | 389 | 1139 | 1102 | 8866 | IndexScan -> Sort -> Lim |
| eq-a+sort-id-desc-lim10 | 10 | 1759 | 388 | 1208 | 1098 | 8451 | IndexScan -> Sort -> Lim |
| eq-c+sort-id-lim10 | 10 | 19992 | 3922 | 12034 | 10947 | 88005 | IndexScan -> Sort -> Lim |
| filter-d+sort-id | 50 | 30000 | 4656 | 11954 | 10898 | 52682 | FullScan(filtered) |
| filter-d+sort-id-desc | 50 | 30000 | 4663 | 11694 | 10917 | 54070 | FullScan(rev)(filtered) |
| filter-d+sort-id-lim10 | 10 | 30000 | 849 | 1970 | 1841 | 7509 | FullScan(filtered) -> Lim |
| range-a+sort-id | 5000 | 30000 | 4819 | 12756 | 11120 | 53940 | FullScan(filtered) |
| range-a+sort-id-lim10 | 10 | 30000 | 17 | 43 | 37 | 203 | FullScan(filtered) -> Lim |
| **Compound filter** |
| eq-ab | 0 | 4 | 9 | 13 | 12 | 102 | IndexScan(a,b) |
| eq-ca | 0 | 4 | 6 | 14 | 11 | 81 | IndexScan(c,a) |
| **Range + sort** |
| range-a+sort-a | 5000 | 87501 | 21645 | 62321 | 60739 | 486727 | IndexScan -> Fetch -> Sort |
| range-a+sort-a-desc | 5000 | 87501 | 21124 | 63473 | 60302 | 490807 | IndexScan(rev) -> Sort |
| range-a+sort-a+lim10 | 10 | 40 | 8 | 23 | 20 | 147 | IndexScan -> Limit(10) |

## Speed Ratios (relative to Local)

| Machine | FullScan(50k) | IdxScan(500) | IdxScan(5k) | Sort(50k) | Lim(10) | Fetch(50k) |
|---------|---------------|--------------|-------------|-----------|---------|------------|
| **Local** | 1.0x | 1.0x | 1.0x | 1.0x | 1.0x | 1.0x |
| **Laptop** | 2.5x | 2.9x | 2.9x | 2.5x | 2.5x | 3.0x |
| **PC** | 2.4x | 3.0x | 2.7x | 2.2x | 2.0x | 2.9x |
| **RPi 4** | 11.5x | 23.7x | 17.0x | 9.9x | 12.8x | 23.7x |

Key: FullScan=fullscan-all, IdxScan(500)=eq-a, IdxScan(5k)=eq-c, Sort=fullscan-sort-d,
Lim=sort-id-lim10, Fetch=sort-a-asc

The RPi is dramatically slower on IndexScan+Fetch (~24x) compared to FullScan (~12x),
suggesting the btree random-access pattern hits the SD card much harder than sequential scan.

## Cost Model Analysis

### Cost/µs Ratios (how many cost units per µs of real time)

The ideal cost model would have a **constant** cost/µs ratio across all queries — meaning
cost perfectly predicts relative execution time.

| Pattern | Local | Laptop | PC | RPi |
|---------|-------|--------|----|-----|
| FullScan (50k) | 6.9 | 2.8 | 2.9 | 0.60 |
| IndexScan fetch (500 docs) | 5.0 | 1.7 | 1.7 | 0.21 |
| IndexScan fetch (5k docs) | 9.0 | 3.1 | 3.4 | 0.53 |
| IndexScan+Fetch (50k sort) | 5.9 | 2.0 | 2.0 | 0.25 |
| FullScan -> Sort (50k) | 9.3 | 3.7 | 4.2 | 0.94 |
| Limit(10) from index | 5.4 | 1.4 | 2.6 | 0.29 |

### Key Observations

1. **Ratios are stable within each machine** — the cost model preserves relative ordering
   correctly. On all 4 machines, the CBO picks the same plans and the ranking holds.

2. **Absolute speed varies 12-24x** across machines, but relative plan costs track correctly.

3. **RPi has very low cost/µs** (~0.2-0.6) meaning each cost unit takes 2-5µs of real time,
   vs Local where each cost unit takes ~0.15-0.2µs. This is expected — the model is
   hardware-independent for *plan selection*.

4. **IndexScan+Fetch is disproportionately expensive on RPi** — the 24x slowdown (vs Local's
   baseline) for random btree access suggests SD card latency dominates. Sequential FullScan
   is only 12x slower. This is consistent: SD cards excel at sequential I/O but suffer on
   random access patterns.

5. **The cost model correctly penalizes Fetch** (cost=4.0/doc for IndexScan+Fetch) which makes
   it prefer FullScan when selectivity is low. This is especially important on slow storage.

### Known Cost Model Issues

| Query | Problem | Impact |
|-------|---------|--------|
| `sort-id-lim10` | cost=30000 but time=4-63µs | FullScan cost doesn't account for Limit early-exit. **Ratio: 473-7264 cost/µs** |
| `eq-a+sort-b+lim10` | cost=1751 but time=9-128µs | IndexSeek cost ignores Limit when sort is covered by index. **Ratio: 14-199 cost/µs** |
| `eq-a+sort-id-lim10` | cost=1759 but time=389-8866µs | CBO uses IndexScan+Sort+Limit. Reading all 500 docs to sort by id is wasteful when FullScan(filtered)+Limit could stop early. |
| `filter-d+sort-id-lim10` | cost=30000 but time=849-7509µs | Cost doesn't reflect Limit benefit on FullScan. |
| `range-a+sort-id-lim10` | cost=30000 but time=17-203µs | 10% filter + Limit(10) means ~100 docs scanned, not 50k. **Worst ratio: 1715 cost/µs** |

### Limit + FullScan Cost Problem

The biggest cost model gap is **FullScan + id-sort + Limit**. Current model assigns full scan cost
(30000) regardless of limit. A better model should account for:

```
effective_docs = min(total_docs, limit / selectivity)
```

| Query | Cost now | Predicted effective_docs | Expected cost | Actual time (Local) |
|-------|----------|------------------------|---------------|---------------------|
| sort-id-lim10 | 30000 | 10 | ~6 | 5 µs |
| range-a+sort-id-lim10 | 30000 | 100 | ~60 | 17 µs |
| filter-d+sort-id-lim10 | 30000 | 10000 | ~6000 | 849 µs |
| filter-d+sort-id | 30000 | 50000 | ~30000 | 4656 µs |

These estimates match actual measured times much better than flat 30000.

### Recommendation: Limit-Aware FullScan Cost

When the plan is `FullScan(id-sorted) + Limit(N)`:

```go
if limit > 0 && fullScanNeedSort == false {
    // Estimate docs to scan before finding `limit` matches
    if selectivity > 0 {
        effectiveDocs = min(totalDocs, float64(limit) / selectivity)
    } else {
        effectiveDocs = float64(limit)  // no filter
    }
    fullScanCost = computeFullScanCost(effectiveDocs, ...)
}
```

Similarly, IndexSeek plans with covered sort + limit should use:
```go
if limit > 0 && sortCoveredByIndex {
    effectiveDocs = min(estimatedYield, float64(limit))
    // Recompute cost with effectiveDocs instead of estimatedYield
}
```

This would fix the massive cost overestimate and allow the CBO to correctly prefer
FullScan(filtered)+Limit over IndexScan+Sort+Limit for `eq-a+sort-id+lim10` cases.
