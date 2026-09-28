#ifndef NETTY_H
#define NETTY_H

#include <stddef.h>
#include <stdint.h>

typedef struct App App;


typedef struct {
    const char *name;
    size_t name_len;
    const char *value;
    size_t value_len;
} http_param_t;

typedef struct {
    const char *name;
    size_t name_len;
    const char *value;
    size_t value_len;
} http_header_t;

typedef struct {
    char client_ip[16];

    const char *method;
    size_t method_len;
    
    const char *path;
    size_t path_len;
    
    const char *query;
    size_t query_len;

    const char *body;
    size_t body_len;

    int keep_alive;
    
    http_param_t params[10];
    int param_count;

    http_header_t headers[32];
    int header_count;
} http_request_t;

typedef struct{
    int status_code;
    const char *content_type;
    int headers_sent;
    
    struct { const char *name; const char *value; } headers[16];
    int header_count;
} http_response_writer_t;

typedef struct http_context {
    App *app;
    int client_fd;
    http_request_t req;
    http_response_writer_t res;
    void *user_data;
    void **handlers;
    int handler_count;
    int current_index;
} http_context_t;

typedef void (*http_handler_t)(http_context_t *ctr);

struct App {
    void *internal_state;

    void (*get)(App *self, const char *path, http_handler_t handler);
    void (*post)(App *self, const char *path, http_handler_t handler);
    void (*put)(App *self, const char *path, http_handler_t handler);
    void (*del)(App *self, const char *path, http_handler_t handler);

    void (*use)(App *self,http_handler_t middleware);

    int (*listen_and_serve)(App *self, const char *host, int num_threads);
    void (*destroy)(App *self);
};

static void default_not_found_handler(http_context_t *ctx);

// PUBLIC API

App http_new_app(void);

void http_next(http_context_t *ctx);
void http_write(http_context_t *ctx, int status_code, const char *content_type, const void *data, size_t len);
void http_serve_file(http_context_t *ctx, const char *filepath, const char *content_type);

const char *http_get_param(http_context_t *ctx, const char *key, size_t *outlen);
const char *http_get_query(http_context_t *ctx, const char *key, size_t *outlen);

const char* http_get_header(http_context_t *ctx, const char *key, size_t *out_len);
void http_set_header(http_context_t *ctx, const char *key, const char *value);

void http_serve_file(http_context_t *ctx, const char *filepath, const char *content_type);
void http_serve_dir(http_context_t *ctx, const char *url_prefix, const char *fs_path);

void http_init_rate_limiter(int max_reqs, int window_sec);
void http_rate_limit_middleware(http_context_t *ctx);


#endif