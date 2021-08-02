// FIO engine to test "file server" access pattern
// I.e. read/write over a number of files sharded over several levels of subdirectories
//
// USAGE: fio -name=test -ioengine=./libfio_fileserver.so -chunk_size=256K \
//   -directory=/home/bench -size=10G [-direct=1] [-fsync_on_close=1] [-sync=1] [-dir_levels=2] [-subdirs_per_dir=64]

#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#define CONFIG_HAVE_GETTID
#define CONFIG_SYNC_FILE_RANGE
#define CONFIG_PWRITEV2
#include "fio/fio.h"
#include "fio/optgroup.h"

struct sec_options;

struct sec_data
{
    int __pad[2];
    struct thread_data *td;
    struct sec_options *opt;

    pthread_mutex_t mutex;
    pthread_cond_t cond, cond_done;
    int finished;
    int in_flight;

    pthread_t *threads;
    int thread_count, thread_alloc;

    struct io_u **requests;
    int request_count, request_alloc;

    struct io_u **done;
    int done_count, done_alloc;
};

struct sec_options
{
    int __pad;
    int __pad2;
    int dir_levels;
    int subdirs_per_dir;
    int chunk_size;
};

static struct fio_option options[] = {
    {
        .name   = "dir_levels",
        .lname  = "dir levels",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, dir_levels),
        .help   = "levels of nested directories (2 by default)",
        .def    = "2",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "subdirs_per_dir",
        .lname  = "subdirectories per directory",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, subdirs_per_dir),
        .help   = "subdirectories per directory (64 by default)",
        .def    = "64",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name   = "chunk_size",
        .lname  = "size of each fileserver chunk (file)",
        .type   = FIO_OPT_INT,
        .off1   = offsetof(struct sec_options, chunk_size),
        .help   = "all I/O will be divided between files of this size (256K by default)",
        .def    = "262144",
        .category = FIO_OPT_C_ENGINE,
        .group  = FIO_OPT_G_FILENAME,
    },
    {
        .name = NULL,
    },
};

static int sec_init(struct thread_data *td)
{
    struct sec_data *bsd;

    struct sec_options *opt = (struct sec_options*)td->eo;
    if (!td->o.directory || opt->chunk_size <= 0 || opt->dir_levels > 0 && opt->subdirs_per_dir <= 0)
    {
        fprintf(stderr, "USAGE: fio -name=test -ioengine=./libfio_fileserver.so"
            " -chunk_size=256K -directory=/home/bench -size=10G [-direct=1] [-fsync_on_close=1] [-sync=1] [-dir_levels=2] [-subdirs_per_dir=64]\n");
        exit(1);
    }

    bsd = calloc(1, sizeof(struct sec_data));
    if (!bsd)
    {
        td_verror(td, errno, "calloc");
        return 1;
    }
    td->io_ops_data = bsd;
    bsd->td = td;
    bsd->opt = opt;

    if (!td->files_index)
    {
        add_file(td, "fileserver", 0, 0);
        td->o.nr_files = td->o.nr_files ? : 1;
        td->o.open_files++;
    }

    if (pthread_mutex_init(&bsd->mutex, NULL) != 0)
    {
        td_verror(td, errno, "pthread_mutex_init");
        return 1;
    }

    if (pthread_cond_init(&bsd->cond, NULL) != 0)
    {
        td_verror(td, errno, "pthread_cond_init");
        return 1;
    }

    if (pthread_cond_init(&bsd->cond_done, NULL) != 0)
    {
        td_verror(td, errno, "pthread_cond_init");
        return 1;
    }

    return 0;
}

static void sec_cleanup(struct thread_data *td)
{
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;
    if (bsd)
    {
        int i;
        void *retval = NULL;
        pthread_mutex_lock(&bsd->mutex);
        bsd->finished = 1;
        pthread_cond_broadcast(&bsd->cond);
        pthread_cond_broadcast(&bsd->cond_done);
        pthread_mutex_unlock(&bsd->mutex);
        for (i = 0; i < bsd->thread_count; i++)
        {
            if (pthread_join(bsd->threads[i], &retval) != 0)
            {
                td_verror(td, errno, "pthread_join");
                exit(1);
            }
        }
        pthread_cond_destroy(&bsd->cond);
        pthread_cond_destroy(&bsd->cond_done);
        pthread_mutex_destroy(&bsd->mutex);
        free(bsd);
    }
}

/* Begin read or write request. */
static void sec_exec(struct sec_data *bsd, struct io_u *io)
{
    struct thread_data *td = bsd->td;
    struct sec_options *opt = bsd->opt;
    int i, r, pos;
    int dirname_len, filename_buf;
    char *file_name;
    struct stat statbuf;
    uint64_t file_idx;

    io->engine_data = bsd;

    file_idx = io->offset / opt->chunk_size;
    dirname_len = strlen(td->o.directory);
    filename_buf = dirname_len + opt->subdirs_per_dir * 3 + 256;
    file_name = malloc(filename_buf);
    if (!file_name)
        exit(1);
    strncpy(file_name, td->o.directory, filename_buf);
    pos = dirname_len;
    for (i = 0; i < opt->dir_levels; i++)
    {
        int subdir = file_idx % opt->subdirs_per_dir;
        file_idx /= opt->subdirs_per_dir;
        pos += snprintf(file_name + pos, filename_buf - pos - 1, "/%02x", subdir);
        r = stat(file_name, &statbuf);
        if (r < 0)
        {
            if (errno == ENOENT)
            {
                if (io->ddir == DDIR_WRITE)
                {
                    r = mkdir(file_name, 0777);
                    if (r < 0 && errno != EEXIST)
                    {
                        fprintf(stderr, "Error mkdir(%s): %d (%s)\n", file_name, errno, strerror(errno));
                        exit(1);
                    }
                }
            }
            else
            {
                fprintf(stderr, "Error stat(%s): %d (%s)\n", file_name, errno, strerror(errno));
                exit(1);
            }
        }
    }
    pos += snprintf(file_name + pos, filename_buf - pos - 1, "/%02lx", file_idx);
    file_name[pos] = 0;

    int fd = open(file_name,
        (io->ddir == DDIR_WRITE ? (O_CREAT|O_RDWR) : O_RDONLY)
        | (td->o.sync_io ? O_SYNC : 0)
        | (td->o.odirect ? O_DIRECT : 0),
        0644
    );
    if (fd < 0 && errno != ENOENT)
    {
        fprintf(stderr, "Error open(%s): %d (%s)\n", file_name, errno, strerror(errno));
        exit(1);
    }
    if (io->ddir == DDIR_READ)
    {
        if (fd >= 0)
        {
            io->error = pread(fd, io->xfer_buf, io->xfer_buflen, io->offset % opt->chunk_size) < 0 ? errno : 0;
        }
        else
        {
            // Read from a non-existing file
            io->error = ENOENT;
        }
    }
    else if (io->ddir == DDIR_WRITE)
    {
        io->error = pwrite(fd, io->xfer_buf, io->xfer_buflen, io->offset % opt->chunk_size) < 0 ? errno : 0;
        if (io->error >= 0 && td->o.fsync_on_close)
        {
            io->error = fsync(fd) < 0 ? errno : 0;
        }
    }
    else if (io->ddir == DDIR_SYNC)
    {
        io->error = fsync(fd) < 0 ? errno : 0;
    }
    else
    {
        io->error = EINVAL;
    }
    close(fd);
    free(file_name);
}

static void* sec_thread(void *opaque)
{
    struct sec_data *bsd = (struct sec_data*)opaque;
    struct io_u* req = NULL;
    while (1)
    {
        // Get request
        pthread_mutex_lock(&bsd->mutex);
        while (bsd->request_count <= 0 && !bsd->finished)
        {
            pthread_cond_wait(&bsd->cond, &bsd->mutex);
        }
        if (bsd->finished)
        {
            pthread_mutex_unlock(&bsd->mutex);
            break;
        }
        req = bsd->requests[--bsd->request_count];
        pthread_mutex_unlock(&bsd->mutex);
        // Execute
        sec_exec(bsd, req);
        // Put response
        pthread_mutex_lock(&bsd->mutex);
        if (bsd->done_count >= bsd->done_alloc)
        {
            bsd->done_alloc += 16;
            bsd->done = realloc(bsd->done, bsd->done_alloc*sizeof(struct io_u*));
        }
        bsd->done[bsd->done_count++] = req;
        if (bsd->done_count == 1)
        {
            pthread_cond_signal(&bsd->cond_done);
        }
        pthread_mutex_unlock(&bsd->mutex);
    }
    return NULL;
}

/* Begin read or write request. */
static enum fio_q_status sec_queue(struct thread_data *td, struct io_u *io)
{
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;

    fio_ro_check(td, io);

    pthread_mutex_lock(&bsd->mutex);
    if (bsd->request_count >= bsd->request_alloc)
    {
        bsd->request_alloc += 16;
        bsd->requests = realloc(bsd->requests, bsd->request_alloc*sizeof(struct io_u*));
    }
    bsd->requests[bsd->request_count++] = io;
    if (bsd->request_count == 1)
    {
        pthread_cond_signal(&bsd->cond);
    }
    pthread_mutex_unlock(&bsd->mutex);

    bsd->in_flight++;
    while (bsd->in_flight > bsd->thread_count)
    {
        if (bsd->thread_count >= bsd->thread_alloc)
        {
            bsd->thread_alloc += 16;
            bsd->threads = realloc(bsd->threads, bsd->thread_alloc*sizeof(pthread_t));
        }
        if (pthread_create(&bsd->threads[bsd->thread_count++], NULL, sec_thread, bsd) != 0)
        {
            td_verror(td, errno, "pthread_create");
            exit(1);
        }
    }

    return FIO_Q_QUEUED;
}

static int sec_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;
    int done_count = 0;
    pthread_mutex_lock(&bsd->mutex);
    while (bsd->done_count <= 0)
    {
        pthread_cond_wait(&bsd->cond_done, &bsd->mutex);
    }
    done_count = bsd->done_count;
    pthread_mutex_unlock(&bsd->mutex);
    return done_count;
}

static struct io_u *sec_event(struct thread_data *td, int event)
{
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;
    struct io_u *req = NULL;
    pthread_mutex_lock(&bsd->mutex);
    if (bsd->done_count > 0)
    {
        req = bsd->done[--bsd->done_count];
    }
    bsd->in_flight--;
    pthread_mutex_unlock(&bsd->mutex);
    return req;
}

static int sec_io_u_init(struct thread_data *td, struct io_u *io)
{
    io->engine_data = NULL;
    return 0;
}

static void sec_io_u_free(struct thread_data *td, struct io_u *io)
{
}

static int sec_open_file(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

static int sec_invalidate(struct thread_data *td, struct fio_file *f)
{
    return 0;
}

struct ioengine_ops ioengine = {
    .name               = "fileserver",
    .version            = FIO_IOOPS_VERSION,
    .flags              = FIO_MEMALIGN | FIO_DISKLESSIO | FIO_NOEXTEND,
    .init               = sec_init,
    .queue              = sec_queue,
    .getevents          = sec_getevents,
    .event              = sec_event,
    .cleanup            = sec_cleanup,
    .open_file          = sec_open_file,
    .invalidate         = sec_invalidate,
    .io_u_init          = sec_io_u_init,
    .io_u_free          = sec_io_u_free,
    .option_struct_size = sizeof(struct sec_options),
    .options            = options,
};

static void fio_init fio_sec_register(void)
{
    register_ioengine(&ioengine);
}

static void fio_exit fio_sec_unregister(void)
{
    unregister_ioengine(&ioengine);
}
