# SQLite (v0.4.6) vs BTree branch — Benchmark Results

500,000 documents, 6 indexes, 3s per scenario.

## Setup Timing

| Step | SQLite | BTree | Speedup |
|------|--------|-------|---------|
| Collection + data | 3.480s | 1.380s | **2.52x** |
| Index (a) | 2.794s | 0.776s | **3.60x** |
| Index (b) | 2.681s | 0.706s | **3.80x** |
| Index (c) | 2.538s | 0.656s | **3.87x** |
| Index (a,b) | 3.674s | 1.018s | **3.61x** |
| Index (a,-b) | 3.736s | 0.875s | **4.27x** |
| Index (email) unique | 2.606s | 1.008s | **2.59x** |
| **Total setup** | **21.5s** | **6.4s** | **3.36x** |

## DB Size

| | SQLite | BTree |
|--|--------|-------|
| After setup | 255.0 MiB + 38.1 MiB WAL | 185.6 MiB + 0 WAL |
| After tests | 306.4 MiB + 38.1 MiB WAL | 371.8 MiB + 0 WAL |

## CRUD Operations

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| FindId | 6,911 | 8,882 | +29% | SQLite |
| Insert | 1,327,879 | 68,778 | **-94.8%** | **BTree** |
| UpdateId | 1,346,687 | 66,729 | **-95.0%** | **BTree** |
| DeleteReinsert | 3,135,055 | 113,630 | **-96.4%** | **BTree** |
| BatchInsert100 | 2,399,192 | 275,416 | **-88.5%** | **BTree** |
| BatchInsert1000 | 5,773,033 | 2,283,546 | **-60.5%** | **BTree** |
| BatchUpdate | 486,215,484 | 1,589,140,041 | +226.8% | SQLite |

## ID Queries

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| IdEq | 9,068 | 9,659 | +7% | ~tie |
| IdIn3 | 19,988 | 12,633 | **-36.8%** | **BTree** |
| IdIn10 | 40,672 | 24,847 | **-38.9%** | **BTree** |
| IdIn100 | 453,839 | 376,702 | **-17.0%** | **BTree** |
| IdIn1000 | 216,306,588 | 150,480,383 | **-30.4%** | **BTree** |

## Full Scan (no indexes, 10k docs)

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| Count | 69,028 | 24,597 | **-64.4%** | **BTree** |
| EqFilter | 3,568,929 | 2,138,093 | **-40.1%** | **BTree** |
| RangeFilter | 3,823,861 | 2,326,700 | **-39.2%** | **BTree** |
| ComplexAndOr | 4,153,558 | 2,525,023 | **-39.2%** | **BTree** |
| NeFilter | 3,589,734 | 2,254,921 | **-37.2%** | **BTree** |

## Simple Index

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| Eq (5k docs) | 25,009,783 | 18,746,874 | **-25.1%** | **BTree** |
| Range (105k docs) | 1,526,522,523 | 1,171,304,755 | **-23.3%** | **BTree** |
| In (25k docs) | 96,960,770 | 104,221,163 | +7.5% | ~tie |
| HighSelectivity | 19,740,127 | 18,544,081 | **-6.1%** | **BTree** |
| LowSelectivity (50k docs) | 67,187,451 | 170,368,691 | +153.6% | SQLite |

## Unique Index

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| Eq | 11,527 | 19,024 | +65.0% | SQLite |
| In3 | 25,829 | 30,822 | +19.3% | SQLite |

## Compound Index

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| FullMatch (100 docs) | 193,735 | 435,259 | +124.6% | SQLite |
| PrefixOnly (5k docs) | 19,566,227 | 18,630,877 | **-4.8%** | ~tie |
| PrefixRange | 16,536,002 | 16,214,180 | **-1.9%** | ~tie |

## Compound Reversed Index

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| FullMatch (100 docs) | 195,310 | 425,171 | +117.6% | SQLite |
| SortAscDesc (limit 100) | 230,142,642 | 136,750,811 | **-40.6%** | **BTree** |
| FilterSort (limit 100) | 1,214,941,042 | 1,170,718,439 | **-3.6%** | ~tie |

## CBO (Cost-Based Optimizer)

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| TwoIdx (500 docs) | 19,757,078 | 18,873,868 | **-4.5%** | ~tie |
| CompoundVsSimple (100 docs) | 191,747 | 424,187 | +121.2% | SQLite |
| ThreeIdx (10 docs) | 194,362 | 433,826 | +123.2% | SQLite |

## Sort

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| NoIdx | 269,169,877 | 254,747,048 | **-5.4%** | ~tie |
| WithIdx | 182,019 | 383,532 | +110.7% | SQLite |
| DescNoIdx | 268,522,574 | 273,957,084 | +2.0% | ~tie |
| DescWithIdx | 183,476 | 377,463 | +105.8% | SQLite |
| LimitNoIdx | 261,109,580 | 265,572,395 | +1.7% | ~tie |
| LimitWithIdx | 36,441 | 56,741 | +55.7% | SQLite |

## Filter + Sort

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| NoIdx | 1,171,173,357 | 1,193,565,504 | +1.9% | ~tie |
| SimpleIdx | 224,294 | 393,381 | +75.4% | SQLite |
| CompoundIdx | 245,012 | 439,451 | +79.4% | SQLite |
| CompoundRevIdx | 236,644 | 411,363 | +73.8% | SQLite |
| WithLimit10 | 52,767 | 67,142 | +27.2% | SQLite |

## Bulk Operations

| Scenario | SQLite ns/op | BTree ns/op | Change | Winner |
|----------|-------------|-------------|--------|--------|
| Update (100 docs) | 10,535,379 | 3,894,072 | **-63.0%** | **BTree** |
| Delete (100 docs) | 15,114,996 | 6,131,769 | **-59.4%** | **BTree** |

## Summary

### BTree wins big (>2x faster)
- **Write operations**: Insert 19x, UpdateId 20x, DeleteReinsert 28x, BatchInsert100 8.7x
- **Full scan**: Count 2.8x, all filters ~1.6x
- **ID $in queries**: 1.3-1.4x across the board
- **Bulk ops**: Update 2.7x, Delete 2.5x
- **Setup**: 3.4x faster data + index creation

### SQLite wins (btree regressions)
- **Indexed sort**: WithIdx 2.1x, DescWithIdx 2.1x, LimitWithIdx 1.6x
- **Filter+Sort with index**: SimpleIdx 1.8x, CompoundIdx 1.8x
- **Compound index point lookups**: FullMatch 2.2x, CBO ThreeIdx 2.2x
- **Unique index**: Eq 1.65x
- **Low selectivity index scan**: 2.5x slower
- **BatchUpdate**: 3.3x slower (allocation pathology)

### Allocation anomalies (BTree)
- Sort/NoIdx: 72 MB vs 8 MB (9x more, but ~same time)
- BatchUpdate: 48,701 allocs vs 48 (1000x more)
- BatchInsert100: 159 KB vs 116 KB per op
