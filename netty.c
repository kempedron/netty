#include "netty.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/uio.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <sys/stat.h>

#define MAX_EVENTS 1024
#define BUFFER_SIZE 8192
#define MAX_FD 65536
#define MAX_MIDDLEWARES 32

typedef struct trie_node{
    char segment[64];
    size_t segment_len;
    int is_param;

    http_handler_t handler_get;
    http_handler_t handler_post;
    http_handler_t handler_put;
    http_handler_t handler_del;

    struct trie_node **children;
    int child_count;
    int child_capacity;
} trie_node_t;

typedef struct {
    trie_node_t *router_root;
    http_handler_t middlewares[MAX_MIDDLEWARES];
    int middleware_count;
} app_state_t;

typedef struct {
    int fd;
    char buffer[BUFFER_SIZE];
    size_t read_pos;
    http_context_t ctx;
} connection_t;

typedef struct {
    App *app;
    int port;
} worker_args_t;


static trie_node_t* new_trie_node(void) {
    trie_node_t *node = calloc(1, sizeof(trie_node_t));
    node->child_capacity = 4;
    node->children = malloc(sizeof(trie_node_t*) * node->child_capacity);
    return node;
}

static void free_trie(trie_node_t *node) {
    if (!node) return;
    for (int i = 0; i < node->child_count; i++) {
        free_trie(node->children[i]);
    }
    free(node->children);
    free(node);
}

static size_t next_segment(const char *path, size_t path_len, size_t offset, const char **seg_start) {
    while (offset < path_len && path[offset] == '/') offset++;
    if (offset >= path_len) return 0;
    
    *seg_start = path + offset;
    size_t len = 0;
    while (offset + len < path_len && path[offset + len] != '/') len++;
    return len;
}

static void add_route(app_state_t *state, const char *method, const char *path, http_handler_t handler) {
    if (!state->router_root) state->router_root = new_trie_node();
    trie_node_t *curr = state->router_root;
    
    const char *seg;
    size_t offset = 0, len;
    
    while ((len = next_segment(path, strlen(path), offset, &seg)) > 0) {
        int is_param = (seg[0] == ':');
        if (is_param) { seg++; len--; } 

        trie_node_t *next_node = NULL;
        for (int i = 0; i < curr->child_count; i++) {
            if (curr->children[i]->is_param == is_param &&
                curr->children[i]->segment_len == len &&
                strncmp(curr->children[i]->segment, seg, len) == 0) {
                next_node = curr->children[i];
                break;
            }
        }
        
        if (!next_node) {
            next_node = new_trie_node();
            strncpy(next_node->segment, seg, len < 63 ? len : 63);
            next_node->segment_len = len < 63 ? len : 63;
            next_node->is_param = is_param;
            
            if (curr->child_count == curr->child_capacity) {
                curr->child_capacity *= 2;
                curr->children = realloc(curr->children, sizeof(trie_node_t*) * curr->child_capacity);
            }
            curr->children[curr->child_count++] = next_node;
        }
        curr = next_node;
        offset = (seg - path) + (is_param ? len + 1 : len);
    }
    
    if (strcmp(method, "GET") == 0) curr->handler_get = handler;
    else if (strcmp(method, "POST") == 0) curr->handler_post = handler;
    else if (strcmp(method, "PUT") == 0) curr->handler_put = handler;
    else if (strcmp(method, "DELETE") == 0) curr->handler_del = handler;
}

void http_next(http_context_t *ctx) {
    if(ctx->current_index < ctx->handler_count) {
        http_handler_t handler = (http_handler_t)ctx->handlers[ctx->current_index++];
        handler(ctx);
    }
}

void http_write(http_context_t *ctx, int status_code, const char *content_type, const void *data, size_t len) {
    if (ctx->res.headers_sent) return;
    
    char headers_buf[512];
    int h_len = snprintf(headers_buf, sizeof(headers_buf),
        "HTTP/1.1 %d OK\r\nServer: C-HTTP\r\nContent-Type: %s\r\nContent-Length: %zu\r\nConnection: %s\r\n\r\n",
        status_code, content_type, len, ctx->req.keep_alive ? "keep-alive" : "close");

    struct iovec iov[2];
    iov[0].iov_base = headers_buf; iov[0].iov_len = h_len;
    iov[1].iov_base = (void*)data; iov[1].iov_len = len;

    (void)writev(ctx->client_fd, iov, 2);
    ctx->res.headers_sent = 1;
}

void http_serve_file(http_context_t *ctx, const char *filepath, const char *content_type) {
    if (ctx->res.headers_sent) return;

    int file_fd = open(filepath, O_RDONLY);
    if (file_fd < 0) {
        http_write(ctx, 404, "text/plain", "404 File Not Found", 18);
        return;
    }

    struct stat stat_buf;
    fstat(file_fd, &stat_buf);
    size_t file_size = stat_buf.st_size;

    char headers_buf[512];
    int h_len = snprintf(headers_buf, sizeof(headers_buf),
        "HTTP/1.1 200 OK\r\n"
        "Server: C-HTTP\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n\r\n",
        content_type, file_size,
        ctx->req.keep_alive ? "keep-alive" : "close");

    write(ctx->client_fd, headers_buf, h_len);

    off_t offset = 0;
    size_t remain = file_size;
    while (remain > 0) {
        ssize_t sent = sendfile(ctx->client_fd, file_fd, &offset, remain);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue; 
            break;
        }
        remain -= sent;
    }

    close(file_fd);
    ctx->res.headers_sent = 1;
}

const char* http_get_param(http_context_t *ctx, const char *key, size_t *out_len) {
    for (int i = 0; i < ctx->req.param_count; i++) {
        if (strlen(key) == ctx->req.params[i].name_len &&
            strncmp(ctx->req.params[i].name, key, ctx->req.params[i].name_len) == 0) {
            *out_len = ctx->req.params[i].value_len;
            return ctx->req.params[i].value;
        }
    }
    return NULL;
}

const char* http_get_query(http_context_t *ctx, const char *key, size_t *out_len) {
    if (!ctx->req.query) return NULL;
    size_t key_len = strlen(key);
    const char *p = ctx->req.query;
    const char *end = p + ctx->req.query_len;

    while (p < end) {
        const char *eq = memchr(p, '=', end - p);
        const char *amp = memchr(p, '&', end - p);
        if (!amp) amp = end;

        if (eq && eq < amp) {
            if ((size_t)(eq - p) == key_len && strncmp(p, key, key_len) == 0) {
                *out_len = amp - (eq + 1);
                return eq + 1;
            }
        }
        p = amp + 1;
    }
    return NULL;
}

static void default_not_found_handler(http_context_t *ctx) {
    http_write(ctx, 404, "text/plain", "404 Not Found", 13);
}

static size_t process_request(connection_t *conn, app_state_t *state) {
    char *buf = conn->buffer;
    size_t buf_len = conn->read_pos;
    
    char *headers_end = strstr(buf, "\r\n\r\n");
    if (!headers_end) return 0;

    size_t headers_len = (headers_end - buf) + 4;
    size_t body_len = 0;

    char *cl_pos = strstr(buf, "Content-Length: ");
    if (!cl_pos) cl_pos = strstr(buf, "content-length: ");
    
    if (cl_pos && cl_pos < headers_end) {
        body_len = strtoul(cl_pos + 16, NULL, 10);
    }
    
    size_t total_req_len = headers_len + body_len;
    if (buf_len < total_req_len) return 0;

    conn->ctx.req.body = buf + headers_len;
    conn->ctx.req.body_len = body_len;

    char *method_end = memchr(buf, ' ', headers_len);
    if (!method_end) return total_req_len;
    char *path_start = method_end + 1;
    char *path_end = memchr(path_start, ' ', headers_len - (path_start - buf));
    if (!path_end) return total_req_len;

    conn->ctx.req.method = buf;
    conn->ctx.req.method_len = method_end - buf;

    char *query_start = memchr(path_start, '?', path_end - path_start);
    if (query_start) {
        conn->ctx.req.path = path_start;
        conn->ctx.req.path_len = query_start - path_start;
        conn->ctx.req.query = query_start + 1;
        conn->ctx.req.query_len = path_end - (query_start + 1);
    } else {
        conn->ctx.req.path = path_start;
        conn->ctx.req.path_len = path_end - path_start;
        conn->ctx.req.query = NULL;
        conn->ctx.req.query_len = 0;
    }

    conn->ctx.req.keep_alive = 1; 
    if (strstr(buf, "Connection: close") || strstr(buf, "connection: close")) {
        conn->ctx.req.keep_alive = 0;
    }

    http_handler_t final_handler = NULL;
    trie_node_t *curr = state->router_root;
    const char *seg;
    size_t offset = 0, len;
    conn->ctx.req.param_count = 0;

    while ((len = next_segment(conn->ctx.req.path, conn->ctx.req.path_len, offset, &seg)) > 0) {
        trie_node_t *next_node = NULL;
        for (int i = 0; i < curr->child_count; i++) {
            if (curr->children[i]->is_param) {
                next_node = curr->children[i];
                if (conn->ctx.req.param_count < 10) {
                    int p_idx = conn->ctx.req.param_count++;
                    conn->ctx.req.params[p_idx].name = next_node->segment;
                    conn->ctx.req.params[p_idx].name_len = next_node->segment_len;
                    conn->ctx.req.params[p_idx].value = seg;
                    conn->ctx.req.params[p_idx].value_len = len;
                }
                break;
            } else if (curr->children[i]->segment_len == len && strncmp(curr->children[i]->segment, seg, len) == 0) {
                next_node = curr->children[i];
                break;
            }
        }
        if (!next_node) { curr = NULL; break; }
        curr = next_node;
        offset = (seg - conn->ctx.req.path) + len;
    }

    if (curr) {
        if (strncmp(conn->ctx.req.method, "GET", conn->ctx.req.method_len) == 0) final_handler = curr->handler_get;
        else if (strncmp(conn->ctx.req.method, "POST", conn->ctx.req.method_len) == 0) final_handler = curr->handler_post;
        else if (strncmp(conn->ctx.req.method, "PUT", conn->ctx.req.method_len) == 0) final_handler = curr->handler_put;
        else if (strncmp(conn->ctx.req.method, "DELETE", conn->ctx.req.method_len) == 0) final_handler = curr->handler_del;
    }

    http_handler_t chain[MAX_MIDDLEWARES + 1];
    int chain_count = 0;
    for (int i = 0; i < state->middleware_count; i++) chain[chain_count++] = state->middlewares[i];
    
    if (final_handler) chain[chain_count++] = final_handler;
    else chain[chain_count++] = default_not_found_handler;

    conn->ctx.client_fd = conn->fd;
    conn->ctx.handlers = (void**)chain;
    conn->ctx.handler_count = chain_count;
    conn->ctx.current_index = 0;
    conn->ctx.res.headers_sent = 0;

    http_next(&conn->ctx);

    return total_req_len;
}

static void set_nonblocking(int fd) {
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

static void optimize_socket(int fd) {
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

static void* reactor_worker(void* arg) {
    worker_args_t *args = (worker_args_t*)arg;
    app_state_t *state = (app_state_t*)args->app->internal_state;
    int port = args->port;
    
    connection_t *connections = calloc(MAX_FD, sizeof(connection_t));
    for (int i = 0; i < MAX_FD; i++) connections[i].fd = -1;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    optimize_socket(server_fd);
    set_nonblocking(server_fd);

    struct sockaddr_in address = { .sin_family = AF_INET, .sin_addr.s_addr = INADDR_ANY, .sin_port = htons(port) };
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) exit(EXIT_FAILURE);
    if (listen(server_fd, SOMAXCONN) < 0) exit(EXIT_FAILURE);

    int epoll_fd = epoll_create1(0);
    struct epoll_event event, events[MAX_EVENTS];
    event.data.fd = server_fd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) < 0) exit(EXIT_FAILURE);

    while (1) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                while (1) {
                    int client_fd = accept(server_fd, NULL, NULL);
                    if (client_fd == -1) break;
                    if (client_fd >= MAX_FD) { close(client_fd); continue; }

                    set_nonblocking(client_fd);
                    optimize_socket(client_fd);
                    
                    struct epoll_event c_event;
                    c_event.data.fd = client_fd;
                    c_event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &c_event);
                    
                    connections[client_fd].fd = client_fd;
                    connections[client_fd].read_pos = 0;
                    connections[client_fd].ctx.app = args->app;
                }
            } else {
                int fd = events[i].data.fd;
                connection_t *conn = &connections[fd];

                if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    close(fd);
                    conn->fd = -1;
                } else if (events[i].events & EPOLLIN) {
                    while (1) {
                        ssize_t bytes = read(fd, conn->buffer + conn->read_pos, BUFFER_SIZE - conn->read_pos - 1);
                        if (bytes == -1) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            close(fd); conn->fd = -1; break;
                        }
                        if (bytes == 0) { close(fd); conn->fd = -1; break; }
                        
                        conn->read_pos += bytes;
                        conn->buffer[conn->read_pos] = '\0';

                        while (conn->read_pos > 0) {
                            size_t consumed = process_request(conn, state);
                            if (consumed == 0) break;

                            if (conn->read_pos > consumed) {
                                memmove(conn->buffer, conn->buffer + consumed, conn->read_pos - consumed);
                                conn->read_pos -= consumed;
                                conn->buffer[conn->read_pos] = '\0';
                            } else {
                                conn->read_pos = 0;
                            }

                            if (!conn->ctx.req.keep_alive) {
                                close(fd);
                                conn->fd = -1;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    free(connections);
    return NULL;
}

static void app_use(App *self, http_handler_t middleware) {
    app_state_t *state = (app_state_t*)self->internal_state;
    if (state->middleware_count < MAX_MIDDLEWARES) state->middlewares[state->middleware_count++] = middleware;
}

static void app_get(App *self, const char *path, http_handler_t handler) { add_route((app_state_t*)self->internal_state, "GET", path, handler); }
static void app_post(App *self, const char *path, http_handler_t handler) { add_route((app_state_t*)self->internal_state, "POST", path, handler); }
static void app_put(App *self, const char *path, http_handler_t handler) { add_route((app_state_t*)self->internal_state, "PUT", path, handler); }
static void app_del(App *self, const char *path, http_handler_t handler) { add_route((app_state_t*)self->internal_state, "DELETE", path, handler); }

static int app_listen_impl(App *self, const char *port_str, int num_threads) {
    int port = atoi(port_str);
    if (num_threads <= 0) {
        num_threads = sysconf(_SC_NPROCESSORS_ONLN);
        if (num_threads <= 0) num_threads = 1;
    }

    printf("Starting Netty C Engine on port %d with %d reactor threads...\n", port, num_threads);

    pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
    worker_args_t *args = malloc(sizeof(worker_args_t) * num_threads);

    for (int i = 0; i < num_threads; i++) {
        args[i].app = self; args[i].port = port;
        pthread_create(&threads[i], NULL, reactor_worker, &args[i]);
    }
    for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);

    free(threads); free(args);
    return 0;
}

static void app_destroy_impl(App *self) {
    app_state_t *state = (app_state_t*)self->internal_state;
    if (state) {
        free_trie(state->router_root);
        free(state);
        self->internal_state = NULL;
    }
}

App http_new_app(void) {
    App app;
    app.internal_state = calloc(1, sizeof(app_state_t));
    app.get = app_get; app.post = app_post; app.put = app_put; app.del = app_del;
    app.use = app_use; app.listen_and_serve = app_listen_impl; app.destroy = app_destroy_impl;
    return app;
}