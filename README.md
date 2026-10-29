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
    0     228      105.94s    99.32%
    1     246      104.69s    98.15%
    2     290      106.19s    99.56%
    3     257      105.90s    99.29%
    4     225      103.89s    97.40%
    5     265      104.01s    97.52%
    6     271      104.67s    98.13%
    7     259      106.53s    99.88%
    8     311      106.55s    99.90%
    9     278      106.66s   100.00%
   10     257      105.24s    98.67%
   11     228      105.17s    98.60%
   12     249      106.22s    99.59%
   13     272      106.61s    99.95%
   14     275      105.57s    98.98%
   15     257      105.86s    99.25%
------------------------------------
hex: 174.423 sec (10.32%)
sha: 23.123 sec (1.37%)
aes: 37.156 sec (2.20%)
zstd: 1454.979 sec (86.11%)

achieved makespan: 106.661 sec
ideal makespan: 105.607 sec
total busy: 1689.713 sec
total util: 99.01%
```
