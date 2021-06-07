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

#ifndef FLB_OUT_FILE_BUFFER
#define FLB_OUT_FILE_BUFFER

#include <monkey/mk_core/mk_list.h>
#include <sys/ipc.h>
#include <msgpack.h>

#define MAX_NAME_LENGTH 256

#define READY 1
#define BUSY 2
#define NEW -1

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
    // Holds a reference to the oldest entry (next entry to be flushed) for a given file.
    struct flb_hash *circular_write_map;
    // The list of entries that are ready to be flushed.
    struct mk_list circular_write_buffer;

    // Key used for the message queue.
    key_t queue_key;
    int queue_id;
};

struct flb_output_write {
    char *path;
    char *tag;
    size_t path_size;
    size_t data_size;
    msgpack_object *data;
    struct flb_time *time;
    struct mk_list _head;
};

static int check_status(struct flb_file_manager *manager, char *path);
static void set_status(struct flb_file_manager *manager, char *path, int status);
static void stage_write(struct flb_file_manager *manager, struct flb_output_write *write);
static void file_buffer_append(struct flb_file_manager *manager, struct flb_output_write *write);
static void prepare_next_append(struct flb_file_manager *manager, struct flb_output_write *write);
static struct flb_output_write *file_get_path_append(struct flb_file_manager *manager, char *path);
static struct flb_output_write *file_get_append(struct flb_file_manager *manager);
static struct flb_file_manager *init_file_manager();
static struct msgpack_object *msgpack_copy(msgpack_object *data);

#endif