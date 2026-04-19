/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implements:
 *   - Long-running supervisor daemon with UNIX domain socket control plane
 *   - Container lifecycle via clone() with PID/UTS/mount namespaces
 *   - Bounded-buffer logging pipeline (producer/consumer with pthreads)
 *   - CLI client that communicates with supervisor over the socket
 *   - Signal handling: SIGCHLD reaping, SIGINT/SIGTERM graceful shutdown
 *   - Integration with kernel monitor module via ioctl
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define MAX_CONTAINERS 64

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    int pipe_fd;           /* read end of container stdout/stderr pipe */
    pthread_t producer_tid;
    int producer_running;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

/* Forward declaration */
struct supervisor_ctx;

typedef struct {
    struct supervisor_ctx *ctx;
    container_record_t *record;
} producer_arg_t;

typedef struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
} supervisor_ctx_t;

static volatile sig_atomic_t got_sigchld = 0;
static volatile sig_atomic_t got_sigterm = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded Buffer Implementation
 * --------------------------------------------------------------- */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * Producer-side insertion into the bounded buffer.
 * Blocks when the buffer is full. Returns 0 on success, -1 on shutdown.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        /* Even during shutdown, allow pushing if there's space so we
         * don't lose final log lines */
        if (buffer->count == LOG_BUFFER_CAPACITY) {
            pthread_mutex_unlock(&buffer->mutex);
            return -1;
        }
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * Consumer-side removal from the bounded buffer.
 * Returns 0 on success, -1 when shutdown is complete and buffer is empty.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * Logging consumer thread.
 * Removes log chunks from the bounded buffer and writes to per-container
 * log files. Exits when shutdown is signaled and all pending items are drained.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    fprintf(stderr, "[logger] Consumer thread started\n");

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        /* Find the container record to get the log path */
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        char path[PATH_MAX];
        path[0] = '\0';
        while (rec) {
            if (strcmp(rec->id, item.container_id) == 0) {
                strncpy(path, rec->log_path, sizeof(path) - 1);
                path[sizeof(path) - 1] = '\0';
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (path[0] == '\0') {
            /* Fallback: construct path from container id */
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        }

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
        }
    }

    fprintf(stderr, "[logger] Consumer thread exiting\n");
    return NULL;
}

/*
 * Producer thread: reads from container's pipe and pushes into bounded buffer.
 */
void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    supervisor_ctx_t *ctx = parg->ctx;
    int pipe_fd = parg->record->pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    strncpy(container_id, parg->record->id, CONTAINER_ID_LEN - 1);
    container_id[CONTAINER_ID_LEN - 1] = '\0';
    free(parg);

    fprintf(stderr, "[producer] Thread started for container %s (pipe_fd=%d)\n",
            container_id, pipe_fd);

    while (1) {
        log_item_t item;
        ssize_t n = read(pipe_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0)
            break;

        item.data[n] = '\0';
        item.length = (size_t)n;
        strncpy(item.container_id, container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(pipe_fd);
    fprintf(stderr, "[producer] Thread exiting for container %s\n", container_id);

    /* Mark producer as done */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, container_id) == 0) {
            rec->producer_running = 0;
            rec->pipe_fd = -1;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return NULL;
}

/*
 * Clone child entrypoint.
 *
 * Isolated PID / UTS / mount context via clone flags.
 * chroot into container rootfs, mount /proc, redirect stdout/stderr,
 * set nice value, exec the command.
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set hostname to container id */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("sethostname");
        _exit(1);
    }

    /* Redirect stdout and stderr to the logging pipe */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        _exit(1);
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        _exit(1);
    }
    if (cfg->log_write_fd != STDOUT_FILENO &&
        cfg->log_write_fd != STDERR_FILENO)
        close(cfg->log_write_fd);

    /* chroot into the container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        _exit(1);
    }
    if (chdir("/") != 0) {
        perror("chdir");
        _exit(1);
    }

    /* Mount proc so ps and /proc work inside container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        /* Non-fatal, continue */
    }

    /* Set nice value */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    /* Execute the command */
    char *argv[] = {cfg->command, NULL};
    char *envp[] = {"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", NULL};
    execve(cfg->command, argv, envp);

    /* If execve fails, try /bin/sh -c */
    char *sh_argv[] = {"/bin/sh", "-c", cfg->command, NULL};
    execve("/bin/sh", sh_argv, envp);

    perror("execve");
    _exit(1);
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ---------------------------------------------------------------
 * Signal handlers
 * --------------------------------------------------------------- */
static void sigchld_handler(int sig)
{
    (void)sig;
    got_sigchld = 1;
}

static void sigterm_handler(int sig)
{
    (void)sig;
    got_sigterm = 1;
}

/* ---------------------------------------------------------------
 * Reap exited children and update metadata
 * --------------------------------------------------------------- */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else
                        rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code = 128 + rec->exit_signal;
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else if (rec->exit_signal == SIGKILL)
                        rec->state = CONTAINER_KILLED;
                    else
                        rec->state = CONTAINER_EXITED;
                }

                fprintf(stderr, "[supervisor] Container %s (pid %d) exited: state=%s code=%d signal=%d\n",
                        rec->id, pid, state_to_string(rec->state),
                        rec->exit_code, rec->exit_signal);

                /* Unregister from kernel monitor */
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, rec->id, pid);

                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ---------------------------------------------------------------
 * Start a container
 * --------------------------------------------------------------- */
static int start_container(supervisor_ctx_t *ctx, const control_request_t *req,
                           char *resp_msg, size_t resp_len)
{
    int pipe_fds[2];
    char *stack;
    pid_t child_pid;
    container_record_t *rec;
    child_config_t cfg;

    /* Check for duplicate container ID */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, req->container_id) == 0 &&
            (rec->state == CONTAINER_RUNNING || rec->state == CONTAINER_STARTING)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(resp_msg, resp_len, "Container %s is already running", req->container_id);
            return -1;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Check rootfs exists */
    struct stat st;
    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        snprintf(resp_msg, resp_len, "rootfs %s does not exist or is not a directory", req->rootfs);
        return -1;
    }

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Create pipe for container stdout/stderr */
    if (pipe(pipe_fds) != 0) {
        snprintf(resp_msg, resp_len, "pipe() failed: %s", strerror(errno));
        return -1;
    }

    /* Prepare child config */
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.id, req->container_id, sizeof(cfg.id) - 1);
    strncpy(cfg.rootfs, req->rootfs, sizeof(cfg.rootfs) - 1);
    strncpy(cfg.command, req->command, sizeof(cfg.command) - 1);
    cfg.nice_value = req->nice_value;
    cfg.log_write_fd = pipe_fds[1];

    /* Allocate stack for clone */
    stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        snprintf(resp_msg, resp_len, "Failed to allocate stack");
        return -1;
    }

    /* Clone with new PID, UTS, and mount namespaces */
    child_pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &cfg);

    if (child_pid < 0) {
        free(stack);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        snprintf(resp_msg, resp_len, "clone() failed: %s", strerror(errno));
        return -1;
    }

    /* Parent: close write end of pipe */
    close(pipe_fds[1]);

    /* Create container record */
    rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        close(pipe_fds[0]);
        free(stack);
        snprintf(resp_msg, resp_len, "Failed to allocate container record");
        return -1;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->pipe_fd = pipe_fds[0];
    rec->stop_requested = 0;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, rec->id, child_pid,
                                  rec->soft_limit_bytes, rec->hard_limit_bytes) != 0) {
            fprintf(stderr, "[supervisor] Warning: failed to register container %s with monitor: %s\n",
                    rec->id, strerror(errno));
        }
    }

    /* Add to container list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Start producer thread for this container */
    producer_arg_t *parg = malloc(sizeof(producer_arg_t));
    if (parg) {
        parg->ctx = ctx;
        parg->record = rec;
        rec->producer_running = 1;
        if (pthread_create(&rec->producer_tid, NULL, producer_thread, parg) != 0) {
            free(parg);
            rec->producer_running = 0;
            fprintf(stderr, "[supervisor] Warning: failed to start producer thread for %s\n", rec->id);
        } else {
            pthread_detach(rec->producer_tid);
        }
    }

    free(stack);

    snprintf(resp_msg, resp_len, "Container %s started (pid %d)", req->container_id, child_pid);
    fprintf(stderr, "[supervisor] %s\n", resp_msg);
    return 0;
}

/* ---------------------------------------------------------------
 * Handle PS command
 * --------------------------------------------------------------- */
static void handle_ps(supervisor_ctx_t *ctx, char *resp_msg, size_t resp_len)
{
    int offset = 0;

    offset += snprintf(resp_msg + offset, resp_len - offset,
                       "%-12s %-8s %-10s %-12s %-10s %-10s %-6s %-6s\n",
                       "CONTAINER", "PID", "STATE", "STARTED", "SOFT_MiB", "HARD_MiB", "EXIT", "SIG");
    offset += snprintf(resp_msg + offset, resp_len - offset,
                       "%-12s %-8s %-10s %-12s %-10s %-10s %-6s %-6s\n",
                       "----------", "------", "--------", "----------", "--------", "--------", "----", "----");

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec && offset < (int)resp_len - 100) {
        char time_buf[20];
        struct tm *tm = localtime(&rec->started_at);
        strftime(time_buf, sizeof(time_buf), "%H:%M:%S", tm);

        offset += snprintf(resp_msg + offset, resp_len - offset,
                           "%-12s %-8d %-10s %-12s %-10lu %-10lu %-6d %-6d\n",
                           rec->id, rec->host_pid, state_to_string(rec->state),
                           time_buf,
                           rec->soft_limit_bytes >> 20,
                           rec->hard_limit_bytes >> 20,
                           rec->exit_code, rec->exit_signal);
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (offset == 0)
        snprintf(resp_msg, resp_len, "No containers tracked\n");
}

/* ---------------------------------------------------------------
 * Handle LOGS command
 * --------------------------------------------------------------- */
static void handle_logs(supervisor_ctx_t *ctx, const char *container_id,
                        char *resp_msg, size_t resp_len)
{
    char log_path[PATH_MAX];
    int found = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, container_id) == 0) {
            strncpy(log_path, rec->log_path, sizeof(log_path) - 1);
            log_path[sizeof(log_path) - 1] = '\0';
            found = 1;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!found) {
        snprintf(resp_msg, resp_len, "Container %s not found", container_id);
        return;
    }

    int fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        snprintf(resp_msg, resp_len, "Log file %s not available yet", log_path);
        return;
    }

    ssize_t n = read(fd, resp_msg, resp_len - 1);
    close(fd);

    if (n <= 0) {
        snprintf(resp_msg, resp_len, "(log file empty)");
    } else {
        resp_msg[n] = '\0';
    }
}

/* ---------------------------------------------------------------
 * Handle STOP command
 * --------------------------------------------------------------- */
static int handle_stop(supervisor_ctx_t *ctx, const char *container_id,
                       char *resp_msg, size_t resp_len)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, container_id) == 0) {
            if (rec->state != CONTAINER_RUNNING && rec->state != CONTAINER_STARTING) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                snprintf(resp_msg, resp_len, "Container %s is not running (state=%s)",
                         container_id, state_to_string(rec->state));
                return -1;
            }
            rec->stop_requested = 1;
            pid_t pid = rec->host_pid;
            pthread_mutex_unlock(&ctx->metadata_lock);

            /* Send SIGTERM first, then SIGKILL after a brief delay */
            kill(pid, SIGTERM);
            fprintf(stderr, "[supervisor] Sent SIGTERM to container %s (pid %d)\n",
                    container_id, pid);

            /* Wait briefly then force kill if still alive */
            usleep(500000); /* 500ms */
            if (kill(pid, 0) == 0) {
                kill(pid, SIGKILL);
                fprintf(stderr, "[supervisor] Sent SIGKILL to container %s (pid %d)\n",
                        container_id, pid);
            }

            snprintf(resp_msg, resp_len, "Stop signal sent to container %s", container_id);
            return 0;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    snprintf(resp_msg, resp_len, "Container %s not found", container_id);
    return -1;
}

/* ---------------------------------------------------------------
 * Handle incoming control request
 * --------------------------------------------------------------- */
static void handle_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    memset(&resp, 0, sizeof(resp));
    n = recv(client_fd, &req, sizeof(req), 0);
    if (n != sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Invalid request");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {
    case CMD_START:
        resp.status = start_container(ctx, &req, resp.message, sizeof(resp.message));
        break;

    case CMD_RUN:
        /* Start the container, then wait for it to finish */
        resp.status = start_container(ctx, &req, resp.message, sizeof(resp.message));
        if (resp.status == 0) {
            /* Send initial ack */
            send(client_fd, &resp, sizeof(resp), 0);

            /* Wait for the container to exit */
            pid_t wait_pid = 0;
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *rec = ctx->containers;
            while (rec) {
                if (strcmp(rec->id, req.container_id) == 0) {
                    wait_pid = rec->host_pid;
                    break;
                }
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (wait_pid > 0) {
                while (1) {
                    usleep(100000); /* 100ms poll */
                    pthread_mutex_lock(&ctx->metadata_lock);
                    rec = ctx->containers;
                    int done = 0;
                    while (rec) {
                        if (strcmp(rec->id, req.container_id) == 0) {
                            if (rec->state != CONTAINER_RUNNING &&
                                rec->state != CONTAINER_STARTING) {
                                done = 1;
                                resp.status = rec->exit_code;
                                snprintf(resp.message, sizeof(resp.message),
                                         "Container %s exited: state=%s code=%d signal=%d",
                                         rec->id, state_to_string(rec->state),
                                         rec->exit_code, rec->exit_signal);
                            }
                            break;
                        }
                        rec = rec->next;
                    }
                    pthread_mutex_unlock(&ctx->metadata_lock);
                    if (done || ctx->should_stop)
                        break;
                }
            }

            /* Send final status */
            send(client_fd, &resp, sizeof(resp), 0);
            return;
        }
        break;

    case CMD_PS:
        handle_ps(ctx, resp.message, sizeof(resp.message));
        resp.status = 0;
        break;

    case CMD_LOGS:
        handle_logs(ctx, req.container_id, resp.message, sizeof(resp.message));
        resp.status = 0;
        break;

    case CMD_STOP:
        resp.status = handle_stop(ctx, req.container_id,
                                  resp.message, sizeof(resp.message));
        break;

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ---------------------------------------------------------------
 * Graceful shutdown: stop all running containers
 * --------------------------------------------------------------- */
static void shutdown_all_containers(supervisor_ctx_t *ctx)
{
    fprintf(stderr, "[supervisor] Shutting down all containers...\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING || rec->state == CONTAINER_STARTING) {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
            fprintf(stderr, "[supervisor] Sent SIGTERM to container %s (pid %d)\n",
                    rec->id, rec->host_pid);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Wait briefly for graceful exit */
    usleep(1000000); /* 1 second */

    /* Force kill any remaining */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING || rec->state == CONTAINER_STARTING) {
            kill(rec->host_pid, SIGKILL);
            fprintf(stderr, "[supervisor] Sent SIGKILL to container %s (pid %d)\n",
                    rec->id, rec->host_pid);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Reap all */
    usleep(500000);
    reap_children(ctx);
}

/* ---------------------------------------------------------------
 * Supervisor main loop
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;
    struct sigaction sa;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    strncpy(ctx.base_rootfs, rootfs, sizeof(ctx.base_rootfs) - 1);

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 1) Open /dev/container_monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] Warning: cannot open /dev/container_monitor: %s\n",
                strerror(errno));
        fprintf(stderr, "[supervisor] Continuing without kernel memory monitoring.\n");
    } else {
        fprintf(stderr, "[supervisor] Kernel monitor connected (fd=%d)\n", ctx.monitor_fd);
    }

    /* 2) Create the UNIX domain socket for control plane */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* Make socket non-blocking so we can poll in the event loop */
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* 3) Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4) Spawn the logger (consumer) thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Started. Listening on %s\n", CONTROL_PATH);
    fprintf(stderr, "[supervisor] Base rootfs: %s\n", rootfs);

    /* 5) Supervisor event loop */
    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv;

        if (got_sigchld) {
            got_sigchld = 0;
            reap_children(&ctx);
        }

        if (got_sigterm) {
            fprintf(stderr, "[supervisor] Received termination signal, shutting down...\n");
            ctx.should_stop = 1;
            break;
        }

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (rc > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                handle_request(&ctx, client_fd);
                close(client_fd);
            }
        }
    }

    /* Shutdown sequence */
    shutdown_all_containers(&ctx);

cleanup:
    /* Signal logger to stop and wait for it */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        container_record_t *next = rec->next;
        if (rec->pipe_fd >= 0)
            close(rec->pipe_fd);
        free(rec);
        rec = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Exited cleanly.\n");
    return 0;
}

/* ---------------------------------------------------------------
 * Client-side: send a control request to the supervisor
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int sock_fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(sock_fd);
        return 1;
    }

    if (send(sock_fd, req, sizeof(*req), 0) != sizeof(*req)) {
        perror("send");
        close(sock_fd);
        return 1;
    }

    /* For CMD_RUN, we receive two responses: initial ack + final status */
    if (req->kind == CMD_RUN) {
        /* Read initial ack */
        n = recv(sock_fd, &resp, sizeof(resp), 0);
        if (n == sizeof(resp)) {
            if (resp.status != 0) {
                fprintf(stderr, "%s\n", resp.message);
                close(sock_fd);
                return resp.status;
            }
            fprintf(stderr, "[run] %s\n", resp.message);
        }

        /* Wait for final status */
        n = recv(sock_fd, &resp, sizeof(resp), 0);
        if (n == sizeof(resp)) {
            printf("%s\n", resp.message);
            close(sock_fd);
            return resp.status;
        }
        close(sock_fd);
        return 1;
    }

    n = recv(sock_fd, &resp, sizeof(resp), 0);
    close(sock_fd);

    if (n != sizeof(resp)) {
        fprintf(stderr, "Failed to receive response from supervisor\n");
        return 1;
    }

    printf("%s\n", resp.message);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
