# libfio_fileserver

This is a loadable engine for [fio](https://github.com/axboe/fio) to test something similar to
the file server access pattern where a number of files is sharded over several levels of subdirectories.

# Building

* Clone this repository
* Clone or symlink the source code directory for your exact `fio` version in `./fio` subdirectory
* Install g++
* Run `make`

# Compatibility

fio 3.15 or later.

# Usage

```
fio -name=test -ioengine=./libfio_fileserver.so -directory=/home/bench \
    -chunk_size=256k -size=10G -direct=1 [-dir_levels=2] [-subdirs_per_dir=64] \
    -bs=256k -rw=randwrite [-fsync_on_close=1] [-sync=1] [-numjobs=16 -group_reporting]
```

Notes:
* `direct=1` is optional, but without it you'll be benchmarking the page cache
* `bs=256k` must be less or equal to `chunk_size`
* `fsync=N` is supported, but it only fsyncs a random file
* reads return error when encountering a non-existing file
* You can also use various distribution parameters (zipf, etc)
* I/O is done using synchronous syscalls, thus iodepth is not supported

Because of the synchronous syscalls it's slightly tricky to use parallelism correctly.
So, for example, to fill a directory with 262144 4k files, run:

```
fio -thread -name=test -direct=1 -ioengine=./libfio_fileserver.so -fsync_on_close=1 \
    -directory=/home/bench -chunk_size=4k -size=1G -bs=4k -rw=write:508k \
    -numjobs=128 -offset_increment=4k -group_reporting
```

# License & Author

* Author: Vitaliy Filippov vitalif[at]yourcmc.ru, 2020
* License: GNU GPL v2.0 or later version
