**Parallelos** is a task scheduler for `massively parallel computing`.
The implementation is very experimental, combined ideas of for what I found from
this [wiki page](https://en.wikipedia.org/wiki/Category:Scheduling_algorithms)

References:

- https://en.wikipedia.org/wiki/List_scheduling
- https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling
- https://en.wikipedia.org/wiki/Shortest_job_next
- https://en.wikipedia.org/wiki/Shortest_remaining_time
- https://en.wikipedia.org/wiki/Highest_response_ratio_next
- https://en.wikipedia.org/wiki/Heterogeneous_earliest_finish_time

Benchmark:

```terminaloutput
# run via cargo bench --bench bench

16 workers, 4096 tasks, payload 58 MB

worker   tasks     busy(s)   utils%
--------------------------------
    0     260     159.462   97.624%
    1     274     159.269   97.506%
    2     233     161.400   98.810%
    3     250     163.343   99.999%
    4     274     156.281   95.676%
    5     274     158.560   97.071%
    6     242     160.319   98.149%
    7     254     161.684   98.984%
    8     265     160.542   98.285%
    9     256     162.756   99.640%
   10     264     156.939   96.079%
   11     268     156.495   95.808%
   12     260     158.153   96.822%
   13     229     161.276   98.734%
   14     251     158.581   97.084%
   15     242     159.745   97.797%
--------------------------------
makespan: 163.344 sec
total busy: 2554.802 sec
total utils: 97.75%

# last updated: 10-08-2026
```
