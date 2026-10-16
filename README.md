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

warming up with 2028 tasks
running 4128 tasks with 16 workers (payload 28 MB)

worker   tasks        busy     util
------------------------------------
    0     254       95.42s    98.31%
    1     241       96.79s    99.71%
    2     288       94.18s    97.03%
    3     223       96.04s    98.94%
    4     237       96.27s    99.18%
    5     251       92.80s    95.60%
    6     250       96.50s    99.42%
    7     273       95.80s    98.69%
    8     260       93.34s    96.16%
    9     267       95.85s    98.75%
   10     259       96.85s    99.78%
   11     289       94.53s    97.39%
   12     257       97.03s    99.96%
   13     243       93.75s    96.58%
   14     239       97.01s    99.93%
   15     297       96.24s    99.15%
------------------------------------
achieved makespan: 97.069 sec
ideal makespan: 95.526 sec
total busy: 1528.413 sec
total util: 98.41%

# last updated: 10-16-2026
```
