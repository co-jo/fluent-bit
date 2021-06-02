/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_hash.h>
#include <msgpack.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <monkey/mk_core/mk_list.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include "file.h"

#ifdef FLB_SYSTEM_WINDOWS
#define NEWLINE "\r\n"
#else
#define NEWLINE "\n"
#endif

#define FILE_MAX 2048
#define MAX_NAME_LENGTH 256

#define READY 1
#define BUSY 0
#define NEW -1

#define mk_list_foreach_debug(curr, head) for( curr = (head)->next; curr != (head); curr = curr->next )
static void debug();

struct queue_message {
    long type;
    long sid;
    char payload[MAX_NAME_LENGTH];
};

struct reply_message {
    long success;
};

struct flb_file_manager {
    // Files able to be written to.
    struct flb_hash *status;
    // Holds list of events to be flushed for each file.
    struct flb_hash *buffer;
    // Holds the number of write events that are ready to be flushed to the file.
    // Each hash table should only include values > 0.
    struct flb_hash *ready_write_events;
    // Holds the number of write events waiting to be flushed to the file.
    struct flb_hash *busy_write_events;
    // Holds a reference to an entry in the circular_write_buffer.
    struct flb_hash *circular_write_map;
    // Each entry holds a reference to the oldest write event to some file.
    struct mk_list circular_write_buffer;

    // Key use for the message queue.
    key_t queue_key;
    int queue_id;

};

struct flb_output_write {
    char *path;
    char *data;
    struct mk_list _head;
};

struct flb_file_conf {
    const char *out_path;
    const char *out_file;
    const char *delimiter;
    const char *label_delimiter;
    const char *template;
    int format;
    int csv_column_names;
    struct flb_output_instance *ins;
    struct flb_file_manager *manager;
};

static void set_status(struct flb_file_manager *manager, char *path, int status) {
    size_t key_size = strlen(path);
    int *status_ptr = flb_malloc(sizeof(int));
    *status_ptr = status;
    flb_hash_add(manager->status, path, key_size, status_ptr, sizeof(int));
}

static int check_status(struct flb_file_manager *manager, char *path) {
    size_t key_size = strlen(path);
    int *status_result = (int*) flb_hash_get_ptr(manager->status, path, key_size);
    if (status_result == NULL) {
        set_status(manager, path, READY);
        return READY;
    } else {
        return *status_result;
    }
}

static void stage_write(struct flb_file_manager *manager, struct flb_output_write *write) {
    if (write == NULL) {
        return;
    }
    flb_info("stage: [path]: %s [data]: %s | [node]: %p, [prev]: %p, [next]: %p", write->path, write->data, &write->_head, &write->_head.prev, &write->_head.next);
    mk_list_add(&write->_head, &manager->circular_write_buffer);
    flb_hash_add(manager->circular_write_map, write->path, strlen(write->path), write, sizeof(struct flb_output_write *));
}

static struct flb_file_manager *init_file_manager() {
    struct flb_file_manager *manager;
    manager = flb_malloc(sizeof(struct flb_file_manager));
    manager->buffer = flb_hash_create(FLB_HASH_EVICT_LESS_USED, 8192, -1);
    manager->status = flb_hash_create(FLB_HASH_EVICT_LESS_USED, 8192, -1);
    manager->ready_write_events = flb_hash_create(FLB_HASH_EVICT_LESS_USED, 8192, -1);
    manager->busy_write_events = flb_hash_create(FLB_HASH_EVICT_LESS_USED, 8192, -1);
    manager->circular_write_map = flb_hash_create(FLB_HASH_EVICT_LESS_USED, 8182, -1);
    mk_list_init(&manager->circular_write_buffer);

    manager->queue_key = ftok("/tmp/FLB_FILE_OUTPUT_PLUGIN", 0);

    manager->queue_id = msgget(manager->queue_key, 0666 | IPC_CREAT);

    return manager;
}

static void file_buffer_append(struct flb_file_manager *manager, struct flb_output_write *write) {
    struct mk_list *head;
    size_t key_size = strlen(write->path);

    // If there is no write for the target file in the circular_buffer, add it directly. 
    struct flb_output_write *staged_write = flb_hash_get_ptr(manager->circular_write_map, write->path, key_size);
    if (staged_write == NULL) {
        stage_write(manager, write);
    } else {
        // Else add it to the buffer/write-queue.
        head = (struct mk_list*) flb_hash_get_ptr(manager->buffer, write->path, key_size);
        if (head == NULL) {
            head = flb_malloc(sizeof(struct mk_list));
            mk_list_init(head);
            flb_hash_add(manager->buffer, write->path, key_size, head, 0);
        }
        mk_list_add(&write->_head, head);
    }
}

static void modify_write_count(struct flb_file_manager *manager, char *path, int val) {
    size_t key_size = strlen(path);

    int status = check_status(manager, path);
    struct flb_hash *hash = status == READY ? manager->ready_write_events : manager->busy_write_events;
    int *count = flb_hash_get_ptr(manager->ready_write_events, path, key_size);
    if (count == NULL) {
        int *zero = flb_malloc(sizeof(int));
        *zero = 0;
        flb_hash_add(hash, path, key_size, zero, sizeof(int));
    }
    count = flb_hash_get_ptr(manager->ready_write_events, path, key_size);
    *count += val;
    assert(*count >= 0);
    if (*count == 0) {
        flb_hash_del(hash, path);
    }
}

static void swap_write_count(struct flb_file_manager *manager, char *path) {
    size_t key_size = strlen(path);

    int status = check_status(manager, path);
    struct flb_hash *hash = status == READY ? manager->ready_write_events : manager->busy_write_events;
    int *count = flb_hash_get_ptr(hash, path, key_size);
    // NOP trying to swap the write count if there are no writes listed.
    if (count == NULL) {
        return;
    }
    if (status == READY) {
        flb_hash_add(manager->busy_write_events, path, key_size, count, sizeof(int));
        flb_hash_del(hash, path);
    } else {
        flb_hash_add(manager->ready_write_events, path, key_size, count, sizeof(int));
        flb_hash_del(hash, path);
    }
}

// Fetch the next write event ready to be flushed.
static struct flb_output_write *file_get_append(struct flb_file_manager *manager) {

    if (mk_list_size(&manager->circular_write_buffer) == 0) {
        return NULL;
    }

    struct flb_output_write *write = mk_list_entry_first(&manager->circular_write_buffer, struct flb_output_write, _head);
    mk_list_del(&write->_head);
    flb_hash_del(manager->circular_write_map, write->path);
    // Now check if there is a new write to replace it.
    struct mk_list *list = (struct mk_list *)flb_hash_get_ptr(manager->buffer, write->path, strlen(write->path));
    if (list != NULL) {
        // Stage the next write.

        struct flb_output_write *to_stage = (struct flb_output_write *) mk_list_entry_first(list, struct flb_output_write, _head);
        mk_list_del(&to_stage->_head);
        stage_write(manager, to_stage);

    }

    return write;
}

// Fetch the next write event for file *path which is ready to be flushed.
static struct flb_output_write *file_get_path_append(struct flb_file_manager *manager, char *path) {
    struct mk_list *head;
    size_t key_size = strlen(path);

    head = (struct mk_list*) flb_hash_get_ptr(manager->buffer, path, key_size);
    if (head == NULL) {
        return NULL;
    }
    struct flb_output_write *write = mk_list_entry_first(head, struct flb_output_write, _head);

    return write;
}

// Get the most recent write event for file *path.
static struct flb_output_write *file_get_last_append(struct flb_file_manager *manager, char *path) {
    struct mk_list *head;
    size_t key_size = strlen(path);

    head = (struct mk_list*) flb_hash_get_ptr(manager->buffer, path, key_size);
    if (head == NULL) {
        return NULL;
    }
    struct flb_output_write *write = mk_list_entry_last(head, struct flb_output_write, _head);

    return write;
}

static int file_append_status(struct flb_file_manager *manager, char *path, int status) {
    size_t key_size = strlen(path);

    struct mk_list *head = (struct mk_list*) flb_hash_get_ptr(manager->buffer, path, key_size);
    if (head == NULL) {
        return 0;
    }
    //swap_write_count(manager, path);
    set_status(manager, path, status);

    return 1;
}

// Updates data-structures to signal that the file mapped by 'path' is not actively appended too.
static int file_pause_appends(struct flb_file_manager *manager, char *path) {
    int append_status = file_append_status(manager, path, BUSY);
    if (!append_status) {
        return -1;
    }
    struct flb_output_write *write = flb_hash_get_ptr(manager->circular_write_map, path, strlen(path));
    mk_list_del(&write->_head);
    return append_status;
}

// Updates data-structures to signal that the file mapped by 'path' is actively appended too.
static int file_resume_appends(struct flb_file_manager *manager, char *path) {
    int append_status = file_append_status(manager, path, READY);
    if (!append_status) {
        return -1;
    }
    struct flb_output_write *write = flb_hash_get_ptr(manager->circular_write_map, path, strlen(path));
    mk_list_init(&write->_head);
    mk_list_add(&write->_head, &manager->circular_write_buffer);
    return append_status;
}

static char *check_delimiter(const char *str)
{
    if (str == NULL) {
        return NULL;
    }

    if (!strcasecmp(str, "\\t") || !strcasecmp(str, "tab")) {
        return "\t";
    }
    else if (!strcasecmp(str, "space")) {
        return " ";
    }
    else if (!strcasecmp(str, "comma")) {
        return ",";
    }

    return NULL;
}

static int cb_file_init(struct flb_output_instance *ins,
                        struct flb_config *config,
                        void *data)
{
    int ret;
    const char *tmp;
    char *ret_str;
    (void) config;
    (void) data;
    struct flb_file_conf *ctx;

    debug();
    ctx = flb_calloc(1, sizeof(struct flb_file_conf));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;
    ctx->format = FLB_OUT_FILE_FMT_JSON; /* default */
    ctx->delimiter = NULL;
    ctx->label_delimiter = NULL;
    ctx->template = NULL;
    ctx->manager = init_file_manager();
    ret = flb_output_config_map_set(ins, (void *) ctx);
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }

    /* Optional, file format */
    tmp = flb_output_get_property("Format", ins);
    if (tmp) {
        if (!strcasecmp(tmp, "csv")) {
            ctx->format    = FLB_OUT_FILE_FMT_CSV;
            ctx->delimiter = ",";
        }
        else if (!strcasecmp(tmp, "ltsv")) {
            ctx->format    = FLB_OUT_FILE_FMT_LTSV;
            ctx->delimiter = "\t";
            ctx->label_delimiter = ":";
        }
        else if (!strcasecmp(tmp, "plain")) {
            ctx->format    = FLB_OUT_FILE_FMT_PLAIN;
            ctx->delimiter = NULL;
            ctx->label_delimiter = NULL;
        }
        else if (!strcasecmp(tmp, "msgpack")) {
            ctx->format    = FLB_OUT_FILE_FMT_MSGPACK;
            ctx->delimiter = NULL;
            ctx->label_delimiter = NULL;
        }
        else if (!strcasecmp(tmp, "template")) {
            ctx->format    = FLB_OUT_FILE_FMT_TEMPLATE;
        }
    }

    tmp = flb_output_get_property("delimiter", ins);
    ret_str = check_delimiter(tmp);
    if (ret_str != NULL) {
        ctx->delimiter = ret_str;
    }

    tmp = flb_output_get_property("label_delimiter", ins);
    ret_str = check_delimiter(tmp);
    if (ret_str != NULL) {
        ctx->label_delimiter = ret_str;
    }

    /* Set the context */
    flb_output_set_context(ins, ctx);

    return 0;
}

static int csv_output(FILE *fp, int column_names,
                      struct flb_time *tm, msgpack_object *obj,
                      struct flb_file_conf *ctx)
{
    int i;
    int map_size;
    msgpack_object_kv *kv = NULL;

    if (obj->type == MSGPACK_OBJECT_MAP && obj->via.map.size > 0) {
        kv = obj->via.map.ptr;
        map_size = obj->via.map.size;

        if (column_names == FLB_TRUE) {
            fprintf(fp, "timestamp%s", ctx->delimiter);
            for (i = 0; i < map_size; i++) {
                msgpack_object_print(fp, (kv+i)->key);
                if (i + 1 < map_size) {
                    fprintf(fp, "%s", ctx->delimiter);
                }
            }
            fprintf(fp, NEWLINE);
        }

        fprintf(fp, "%lld.%.09ld%s",
                (long long) tm->tm.tv_sec, tm->tm.tv_nsec, ctx->delimiter);

        for (i = 0; i < map_size - 1; i++) {
            msgpack_object_print(fp, (kv+i)->val);
            fprintf(fp, "%s", ctx->delimiter);
        }

        msgpack_object_print(fp, (kv+(map_size-1))->val);
        fprintf(fp, NEWLINE);
    }
    return 0;
}

static int ltsv_output(FILE *fp, struct flb_time *tm, msgpack_object *obj,
                       struct flb_file_conf *ctx)
{
    msgpack_object_kv *kv = NULL;
    int i;
    int map_size;

    if (obj->type == MSGPACK_OBJECT_MAP && obj->via.map.size > 0) {
        kv = obj->via.map.ptr;
        map_size = obj->via.map.size;
        fprintf(fp, "\"time\"%s%f%s",
                ctx->label_delimiter,
                flb_time_to_double(tm),
                ctx->delimiter);

        for (i = 0; i < map_size - 1; i++) {
            msgpack_object_print(fp, (kv+i)->key);
            fprintf(fp, "%s", ctx->label_delimiter);
            msgpack_object_print(fp, (kv+i)->val);
            fprintf(fp, "%s", ctx->delimiter);
        }

        msgpack_object_print(fp, (kv+(map_size-1))->key);
        fprintf(fp, "%s", ctx->label_delimiter);
        msgpack_object_print(fp, (kv+(map_size-1))->val);
        fprintf(fp, NEWLINE);
    }
    return 0;
}

static int template_output_write(struct flb_file_conf *ctx,
                                 FILE *fp, struct flb_time *tm, msgpack_object *obj,
                                 const char *key, int size)
{
    int i;
    msgpack_object_kv *kv;

    /*
     * Right now we treat "{time}" specially and fill the placeholder
     * with the metadata timestamp (formatted as float).
     */
    if (!strncmp(key, "time", size)) {
        fprintf(fp, "%f", flb_time_to_double(tm));
        return 0;
    }

    if (obj->type != MSGPACK_OBJECT_MAP) {
        flb_plg_error(ctx->ins, "invalid object type (type=%i)", obj->type);
        return -1;
    }

    for (i = 0; i < obj->via.map.size; i++) {
        kv = obj->via.map.ptr + i;

        if (size != kv->key.via.str.size) {
            continue;
        }

        if (!memcmp(key, kv->key.via.str.ptr, size)) {
            if (kv->val.type == MSGPACK_OBJECT_STR) {
                fwrite(kv->val.via.str.ptr, 1, kv->val.via.str.size, fp);
            }
            else {
                msgpack_object_print(fp, kv->val);
            }
            return 0;
        }
    }
    return -1;
}

/*
 * Python-like string templating for out_file.
 *
 * This accepts a format string like "my name is {name}" and fills
 * placeholders using corresponding values in a record.
 *
 * e.g. {"name":"Tom"} => "my name is Tom"
 */
static int template_output(FILE *fp, struct flb_time *tm, msgpack_object *obj,
                           struct flb_file_conf *ctx)
{
    int i;
    int len = strlen(ctx->template);
    int keysize;
    const char *key;
    const char *pos;
    const char *inbrace = NULL;  /* points to the last open brace */

    for (i = 0; i < len; i++) {
        pos = ctx->template + i;
        if (*pos == '{') {
            if (inbrace) {
                /*
                 * This means that we find another open brace inside
                 * braces (e.g. "{a{b}"). Ignore the previous one.
                 */
                fwrite(inbrace, 1, pos - inbrace, fp);
            }
            inbrace = pos;
        }
        else if (*pos == '}' && inbrace) {
            key = inbrace + 1;
            keysize = pos - inbrace - 1;

            if (template_output_write(ctx, fp, tm, obj, key, keysize)) {
                fwrite(inbrace, 1, pos - inbrace + 1, fp);
            }
            inbrace = NULL;
        }
        else {
            if (!inbrace) {
                fputc(*pos, fp);
            }
        }
    }

    /* Handle an unclosed brace like "{abc" */
    if (inbrace) {
        fputs(inbrace, fp);
    }
    fputs(NEWLINE, fp);
    return 0;
}


static int plain_output(FILE *fp, msgpack_object *obj, size_t alloc_size)
{
    char *buf;

    buf = flb_msgpack_to_json_str(alloc_size, obj);
    if (buf) {
        fprintf(fp, "%s" NEWLINE,
                buf);
        flb_free(buf);
    }
    return 0;
}

static void print_circular_buffer(struct flb_file_manager *manager) {
    struct mk_list *head;
    flb_info("[print-circular-buffer]:");
    mk_list_foreach(head, &manager->circular_write_buffer) {
        struct flb_output_write *item = mk_list_entry(head, struct flb_output_write, _head);
        flb_info("\t[path]: %s [data]: %s", item->path, item->data);
    }
    flb_info("");
}

static void print_write_buffer(struct flb_file_manager *manager, char *path) {
    struct mk_list *head;

    int limit = 10;
    size_t key_size = strlen(path);
    struct mk_list *queue = (struct mk_list *) flb_hash_get_ptr(manager->buffer, path, key_size);
    flb_info("[print-write-buffer]:");
    if (queue != NULL) {
        int count = 0;
        mk_list_foreach(head, queue) {
            struct flb_output_write *item = mk_list_entry(head, struct flb_output_write, _head);
            flb_info("\t[path]: %s [data]: %s", item->path, item->data);
            count += 1;
            if (count > limit) {
                break;
            }
        }
    }
    flb_info("");
}


static void debug() {
    struct flb_file_manager *manager;
    manager = init_file_manager();

    struct flb_output_write a0 = { "a.log", "a0" };
    struct flb_output_write a1 = { "a.log", "a1" };
    struct flb_output_write a2 = { "a.log", "a2" };
    struct flb_output_write a3 = { "a.log", "a3" };

    file_buffer_append(manager, &a0);
    print_write_buffer(manager, "a.log");
    print_circular_buffer(manager);
    file_buffer_append(manager, &a1);
    print_write_buffer(manager, "a.log");
    print_circular_buffer(manager);
    file_buffer_append(manager, &a2);
    print_write_buffer(manager, "a.log");
    print_circular_buffer(manager);
    file_buffer_append(manager, &a3);
    print_write_buffer(manager, "a.log");
    print_circular_buffer(manager);
    struct flb_output_write b1 = { "b.log", "b1" };
    file_buffer_append(manager, &b1);
    print_write_buffer(manager, "b.log");
    print_circular_buffer(manager);

    struct flb_output_write *write = file_get_append(manager);
    flb_info("write: %s -> %s", write->path, write->data);
    print_circular_buffer(manager);
    print_write_buffer(manager, "a.log");
}

static void cb_file_flush(const void *data, size_t bytes,
                          const char *tag, int tag_len,
                          struct flb_input_instance *i_ins,
                          void *out_context,
                          struct flb_config *config)
{
    int ret;
    int column_names;
    FILE * fp;
    msgpack_unpacked result;
    size_t off = 0;
    size_t last_off = 0;
    size_t alloc_size = 0;
    size_t total;
    char out_file[PATH_MAX];
    char *buf;
    char *tag_buf;
    long file_pos;
    msgpack_object *obj;
    struct flb_file_conf *ctx = out_context;
    struct flb_time tm;
    (void) i_ins;
    (void) config;

    /* Set the right output file */
    if (ctx->out_path) {
        if (ctx->out_file) {
            snprintf(out_file, PATH_MAX - 1, "%s/%s",
                     ctx->out_path, ctx->out_file);
        }
        else {
            snprintf(out_file, PATH_MAX - 1, "%s/%s",
                     ctx->out_path, tag);
        }
    }
    else {
        if (ctx->out_file) {
            snprintf(out_file, PATH_MAX - 1, "%s", ctx->out_file);
        }
        else {
            snprintf(out_file, PATH_MAX - 1, "%s", tag);
        }
    }

    /* Open output file with default name as the Tag */
    fp = fopen(out_file, "ab+");
    if (fp == NULL) {
        flb_errno();
        flb_plg_error(ctx->ins, "error opening: %s", out_file);
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }

    /*
     * Get current file stream position, we gather this in case 'csv' format
     * needs to write the column names.
     */
    file_pos = ftell(fp);

    tag_buf = flb_malloc(tag_len + 1);
    if (!tag_buf) {
        flb_errno();
        fclose(fp);
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }
    memcpy(tag_buf, tag, tag_len);
    tag_buf[tag_len] = '\0';

    /*
     * Msgpack output format used to create unit tests files, useful for
     * Fluent Bit developers.
     */
    if (ctx->format == FLB_OUT_FILE_FMT_MSGPACK) {
        off = 0;
        total = 0;

        do {
            ret = fwrite((char *)data + off, 1, bytes - off, fp);
            if (ret < 0) {
                flb_errno();
                fclose(fp);
                flb_free(tag_buf);
                FLB_OUTPUT_RETURN(FLB_RETRY);
            }
            total += ret;
        } while (total < bytes);

        fclose(fp);
        flb_free(tag_buf);
        FLB_OUTPUT_RETURN(FLB_OK);
    }

    struct queue_message msg;
    int qid = ctx->manager->queue_id;
    if (qid == -1) {
        perror("ctx->manager->queue_id");
    }
    if (msgrcv(qid, &msg, sizeof(struct queue_message), 0, IPC_NOWAIT) <= 0) {
        flb_info("no msgrcv.");
    } else {
        flb_info("msgrcv: %s %d %s", &msg.type, msg.sid, &msg.payload);
        // Reply.
        struct reply_message reply;
        reply.success = 1;
        if (msgsnd(msg.sid, &reply, sizeof(struct reply_message), 0) == -1) {
            perror("server::reply");
        }
    }
    /*
     * Upon flush, for each array, lookup the time and the first field
     * of the map to use as a data point.
     */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        alloc_size = (off - last_off) + 128; /* JSON is larger than msgpack */
        last_off = off;

        flb_time_pop_from_msgpack(&tm, &result, &obj);

        switch (ctx->format){
        case FLB_OUT_FILE_FMT_JSON:
            buf = flb_msgpack_to_json_str(alloc_size, obj);
            struct flb_output_write *write;
            write = flb_malloc(sizeof(struct flb_output_write));

            char *write_path;
            write_path = flb_malloc(sizeof(out_file));
            memcpy(write_path, &out_file, sizeof(out_file));
            write->path = write_path;

            char *write_buf;
            write_buf = flb_malloc(alloc_size);
            memcpy(write_buf, buf, alloc_size);
            write->data = write_buf;

            file_buffer_append(ctx->manager, write);

            int status = check_status(ctx->manager, write->path);
            print_circular_buffer(ctx->manager);
            print_write_buffer(ctx->manager, write->path);

            struct mk_list *head = (struct mk_list*) flb_hash_get_ptr(ctx->manager->buffer, write->path, strlen(write->path));
            if (head != NULL && mk_list_size(head) > 1) {
                struct flb_output_write *flush = file_get_append(ctx->manager);
                flb_info("[flush]: %s -> %s", flush->data, flush->path);
            }

            if (buf) {
                fprintf(fp, "%s: [%"PRIu64".%09lu, %s]" NEWLINE,
                        tag_buf,
                        tm.tm.tv_sec, tm.tm.tv_nsec,
                        buf);
                flb_free(buf);
            }
            else {
                msgpack_unpacked_destroy(&result);
                fclose(fp);
                flb_free(tag_buf);
                FLB_OUTPUT_RETURN(FLB_RETRY);
            }
            break;
        case FLB_OUT_FILE_FMT_CSV:
            if (ctx->csv_column_names == FLB_TRUE && file_pos == 0) {
                column_names = FLB_TRUE;
                file_pos = 1;
            }
            else {
                column_names = FLB_FALSE;
            }
            csv_output(fp, column_names, &tm, obj, ctx);
            break;
        case FLB_OUT_FILE_FMT_LTSV:
            ltsv_output(fp, &tm, obj, ctx);
            break;
        case FLB_OUT_FILE_FMT_PLAIN:
            plain_output(fp, obj, alloc_size);
            break;
        case FLB_OUT_FILE_FMT_TEMPLATE:
            template_output(fp, &tm, obj, ctx);
            break;
        }
    }

    flb_free(tag_buf);
    msgpack_unpacked_destroy(&result);
    fclose(fp);

    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_file_exit(void *data, struct flb_config *config)
{
    struct flb_file_conf *ctx = data;

    if (!ctx) {
        return 0;
    }

    flb_free(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "path", NULL,
     0, FLB_TRUE, offsetof(struct flb_file_conf, out_path),
     "Absolute path to store the files. This parameter is optional"
    },

    {
     FLB_CONFIG_MAP_STR, "file", NULL,
     0, FLB_TRUE, offsetof(struct flb_file_conf, out_file),
     "Name of the target file to write the records. If 'path' is specified, "
     "the value is prefixed"
    },

    {
     FLB_CONFIG_MAP_STR, "format", NULL,
     0, FLB_FALSE, 0,
     "Specify the output data format, the available options are: plain (json), "
     "csv, ltsv and template. If no value is set the outgoing data is formatted "
     "using the tag and the record in json"
    },

    {
     FLB_CONFIG_MAP_STR, "delimiter", NULL,
     0, FLB_FALSE, 0,
     "Set a custom delimiter for the records"
    },

    {
     FLB_CONFIG_MAP_STR, "label_delimiter", NULL,
     0, FLB_FALSE, 0,
     "Set a custom label delimiter, to be used with 'ltsv' format"
    },

    {
     FLB_CONFIG_MAP_STR, "template", "{time} {message}",
     0, FLB_TRUE, offsetof(struct flb_file_conf, template),
     "Set a custom template format for the data"
    },

    {
     FLB_CONFIG_MAP_BOOL, "csv_column_names", "false",
     0, FLB_TRUE, offsetof(struct flb_file_conf, csv_column_names),
     "Add column names (keys) in the first line of the target file"
    },

    /* EOF */
    {0}
};

struct flb_output_plugin out_file_plugin = {
    .name         = "file",
    .description  = "Generate log file",
    .cb_init      = cb_file_init,
    .cb_flush     = cb_file_flush,
    .cb_exit      = cb_file_exit,
    .config_map   = config_map,
    .flags        = 0,
};
