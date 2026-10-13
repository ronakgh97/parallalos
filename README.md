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
    0     237      150.44s    99.33%
    1     256      144.84s    95.64%
    2     279      148.45s    98.02%
    3     249      150.34s    99.27%
    4     272      146.02s    96.42%
    5     263      151.44s   100.00%
    6     268      150.37s    99.29%
    7     254      151.25s    99.87%
    8     257      143.99s    95.08%
    9     253      148.09s    97.78%
   10     247      150.64s    99.47%
   11     246      148.77s    98.23%
   12     231      150.40s    99.31%
   13     241      149.89s    98.97%
   14     275      149.08s    98.44%
   15     268      149.21s    98.53%
------------------------------------
achieved makespan: 151.446 sec
ideal makespan: 148.950 sec
total busy: 2383.202 sec
total util: 98.35%

# last updated: 10-13-2026
```
