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

#define CONFIG_HAVE_GETTID
#define CONFIG_SYNC_FILE_RANGE
#define CONFIG_PWRITEV2
#include "fio/fio.h"
#include "fio/optgroup.h"

struct sec_data
{
    int __pad[2];
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

    bsd = malloc(sizeof(struct sec_data));
    if (!bsd)
    {
        td_verror(td, errno, "calloc");
        return 1;
    }
    td->io_ops_data = bsd;

    if (!td->files_index)
    {
        add_file(td, "fileserver", 0, 0);
        td->o.nr_files = td->o.nr_files ? : 1;
        td->o.open_files++;
    }

    return 0;
}

static void sec_cleanup(struct thread_data *td)
{
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;
    if (bsd)
    {
        free(bsd);
    }
}

/* Begin read or write request. */
static enum fio_q_status sec_queue(struct thread_data *td, struct io_u *io)
{
    struct sec_options *opt = (struct sec_options*)td->eo;
    struct sec_data *bsd = (struct sec_data*)td->io_ops_data;
    int i, r, pos;
    int dirname_len, filename_buf;
    char *file_name;
    struct stat statbuf;
    uint64_t file_idx;

    fio_ro_check(td, io);

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
        | (td->o.odirect ? O_DIRECT : 0)
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
    return FIO_Q_COMPLETED;
}

static int sec_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *t)
{
    return 0;
}

static struct io_u *sec_event(struct thread_data *td, int event)
{
    return NULL;
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
