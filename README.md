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

running 4096 tasks with 16 workers (payload 50 MB)

worker   tasks        busy     util
------------------------------------
    0     271      141.88s    91.76%
    1     269      152.62s    98.71%
    2     252      154.47s    99.90%
    3     271      148.03s    95.73%
    4     218      151.70s    98.11%
    5     285      150.59s    97.39%
    6     244      150.20s    97.14%
    7     217      151.52s    97.99%
    8     297      145.06s    93.81%
    9     233      153.83s    99.49%
   10     265      152.04s    98.33%
   11     272      145.29s    93.96%
   12     229      151.67s    98.09%
   13     237      154.62s   100.00%
   14     263      149.00s    96.36%
   15     273      147.61s    95.46%
------------------------------------
achieved makespan: 154.625 sec
ideal makespan: 150.008 sec
total busy: 2400.132 sec
total util: 97.01%

# last updated: 10-09-2026
```
