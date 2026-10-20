**Parallelos** is a task scheduler for `massively parallel computing`.
The implementation & API is **very experimental (edge/deadlock cases are documented)**,
combined ideas of for what I found from
this [wiki page](https://en.wikipedia.org/wiki/Category:Scheduling_algorithms)

Currently, the scheduler more or less implements `list scheduling` algorithm with `longest-processing-time-first` (LPT)
heuristic.

> Nested parallelism is not implemented, anything weird can lead to deadlocks.

References:

- https://en.wikipedia.org/wiki/List_scheduling
- https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling
- https://en.wikipedia.org/wiki/Optimal_job_scheduling
- https://en.wikipedia.org/wiki/Identical-machines_scheduling
- https://en.wikipedia.org/wiki/Uniform-machines_scheduling
- https://en.wikipedia.org/wiki/Shortest_job_next
- https://en.wikipedia.org/wiki/Shortest_remaining_time
- https://en.wikipedia.org/wiki/Highest_response_ratio_next
- https://en.wikipedia.org/wiki/Heterogeneous_earliest_finish_time

Benchmark:

```terminaloutput
# run via cargo bench --bench bench

warming up with 1536 tasks
running 4168 tasks with 16 workers (payload 34 MB)

worker   tasks        busy     util
------------------------------------
    0     265      113.12s    99.52%
    1     292      112.38s    98.88%
    2     235      112.56s    99.03%
    3     244      112.96s    99.38%
    4     234      111.15s    97.79%
    5     241      112.12s    98.65%
    6     268      113.66s   100.00%
    7     249      111.81s    98.37%
    8     238      112.59s    99.06%
    9     287      113.41s    99.78%
   10     261      112.51s    98.99%
   11     282      112.43s    98.92%
   12     293      113.50s    99.86%
   13     239      111.15s    97.79%
   14     286      113.55s    99.90%
   15     254      112.20s    98.71%
------------------------------------
hex: 171.786 sec (9.54%) 
sha: 22.062 sec (1.22%) 
aes: 42.646 sec (2.37%) 
zstd: 1564.586 sec (86.87%)

achieved makespan: 113.663 sec
ideal makespan: 112.570 sec
total busy: 1801.113 sec
total util: 99.04%

# last updated: 10-20-2026
```
