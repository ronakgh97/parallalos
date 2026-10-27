**Parallelos** is a task scheduler for `massively parallel computing`.
The implementation & API is **very experimental (edge/deadlock cases are documented)**,
combined ideas of for what I found from
this [wiki page](https://en.wikipedia.org/wiki/Category:Scheduling_algorithms)

Currently, the scheduler more or less implements `list scheduling` algorithm with `longest-processing-time-first` (LPT)
heuristic.

> Nested parallelism is not implemented, anything weird or funny can lead to deadlocks 💀.

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
    0     284      103.45s   100.00%
    1     241      100.99s    97.62%
    2     281      102.95s    99.52%
    3     270      103.22s    99.77%
    4     258      102.67s    99.24%
    5     228      101.10s    97.73%
    6     241      101.63s    98.24%
    7     224      101.59s    98.20%
    8     256      101.94s    98.53%
    9     261      101.99s    98.59%
   10     255      103.03s    99.60%
   11     284      103.08s    99.64%
   12     264      101.18s    97.80%
   13     258      102.77s    99.34%
   14     284      102.72s    99.29%
   15     279      102.51s    99.09%
------------------------------------
hex: 173.694 sec (10.61%)
sha: 24.152 sec (1.48%)
aes: 37.029 sec (2.26%)
zstd: 1401.916 sec (85.65%)

achieved makespan: 103.453 sec
ideal makespan: 102.301 sec
total busy: 1636.823 sec
total util: 98.89%
```
