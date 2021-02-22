# S3 multiget benchmarks

This repository contains some benchmarks for S3 multigets, inspired by
work on [llama](https://github.com/nelhage/llama/). In particular, it
exists to test the effectiveness of HTTP pipelining on S3 multiget
latency.

# Results

I tested this code from Amazon Lambda in s3, downloading 500 randomly
selected header files from linux v5.10's `include/linux/`
directory. These files and their blake2b sums are listed in the
[`TEST_OBJECTS`](blob/main/TEST_OBJECTS) file.

I then ran this code, in both pipelined and "concurrent" modes, for a
range of threads from 10 to 100. You can see the data in
[`data.csv`](blob/main/data.csv), or summarized as a plot:

![Plot of data](img/data.png)
