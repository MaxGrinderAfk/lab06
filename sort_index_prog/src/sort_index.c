#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>
#include <assert.h>

typedef struct index_s {
    double time_mark;
    uint64_t recno;
} index_s;

typedef struct index_hdr_s {
    uint64_t records;
    index_s idx[];
} index_hdr_s;

typedef struct {
    int thread_id;
    char *buffer;
    size_t block_size;
    int num_blocks;
    int *block_map;
    int *processed_blocks;
    int *current_merge_pairs;
    size_t *merge_block_size;
    int *merge_blocks_count;
    pthread_barrier_t *barrier;
    pthread_mutex_t *map_mutex;
} thread_params_t;

int compare_index(const void *a, const void *b) {
    const index_s *ia = (const index_s *)a;
    const index_s *ib = (const index_s *)b;

    if (ia->time_mark < ib->time_mark) return -1;
    if (ia->time_mark > ib->time_mark) return 1;
    
    return (ia->recno < ib->recno) ? -1 : (ia->recno > ib->recno);
}

void merge_blocks(index_s *block1, index_s *block2, size_t block_size) {
    size_t total = block_size * 2;
    index_s *merged = malloc(total * sizeof(index_s));
    if (!merged) {
        perror("malloc failed");
        exit(EXIT_FAILURE);
    }

    size_t i = 0, j = 0, k = 0;
    while (i < block_size && j < block_size) {
        if (block1[i].time_mark < block2[j].time_mark ||
           (block1[i].time_mark == block2[j].time_mark && block1[i].recno < block2[j].recno)) {
            merged[k++] = block1[i++];
        } else {
            merged[k++] = block2[j++];
        }
    }

    while (i < block_size) merged[k++] = block1[i++];
    while (j < block_size) merged[k++] = block2[j++];

    for (size_t idx = 0; idx < total; idx++) {
        block1[idx] = merged[idx];
    }

    free(merged);
}

int get_next_block(thread_params_t *params) {
    int next_block = -1;
    pthread_mutex_lock(params->map_mutex);
    for (int i = 0; i < params->num_blocks; ++i) {
        if (!params->block_map[i]) {
            params->block_map[i] = 1;
            next_block = i;
            break;
        }
    }
    pthread_mutex_unlock(params->map_mutex);
    return next_block;
}

void *thread_func(void *arg) {
    thread_params_t *params = (thread_params_t *)arg;
    pthread_barrier_wait(params->barrier);

    int current_block = get_next_block(params);
    while (current_block != -1) {
        index_s *block = (index_s *)(params->buffer + current_block * params->block_size);
        if (params->block_size % sizeof(index_s) != 0) {
    fprintf(stderr, "ERROR: block_size is not aligned to index_s!\n");
    exit(EXIT_FAILURE);
}

        qsort(block, params->block_size / sizeof(index_s), sizeof(index_s), compare_index);
        current_block = get_next_block(params);
    }

    pthread_barrier_wait(params->barrier);

    while (*(params->merge_blocks_count) > 1) {
        while (1) {
            pthread_mutex_lock(params->map_mutex);
            int pairs = *(params->merge_blocks_count) / 2;
            int my_pair = -1;
            if (*params->current_merge_pairs < pairs) {
                my_pair = (*params->current_merge_pairs)++;
            }
            pthread_mutex_unlock(params->map_mutex);

            if (my_pair == -1) break;

            index_s *b1 = (index_s *)(params->buffer + my_pair * 2 * (*params->merge_block_size));
            index_s *b2 = (index_s *)(params->buffer + (my_pair * 2 + 1) * (*params->merge_block_size));
            merge_blocks(b1, b2, *params->merge_block_size / sizeof(index_s));
        }

        pthread_barrier_wait(params->barrier);

        pthread_mutex_lock(params->map_mutex);
        if (params->thread_id == 0) {
            if (*(params->merge_blocks_count) % 2 == 1) {
                size_t src_idx = *(params->merge_blocks_count) - 1;
                size_t dst_idx = src_idx / 2;
                
                index_s *src = (index_s *)(params->buffer + src_idx * (*params->merge_block_size));
                index_s *dst = (index_s *)(params->buffer + dst_idx * 2 * (*params->merge_block_size));
                for (size_t i = 0; i < (*params->merge_block_size) / sizeof(index_s); i++) {
                    dst[i] = src[i];
                }
            }

            *(params->merge_blocks_count) = (*(params->merge_blocks_count) + 1) / 2;
            *(params->merge_block_size) *= 2;
            *(params->current_merge_pairs) = 0;
        }
        pthread_mutex_unlock(params->map_mutex);

        if (params->num_blocks > 1) {
            pthread_barrier_wait(params->barrier);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s memsize blocks threads filename\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t memsize = strtoul(argv[1], NULL, 10);
    int blocks = atoi(argv[2]);
    int threads = atoi(argv[3]);
    const char *filename = argv[4];

    long page_size = sysconf(_SC_PAGESIZE);
    if (memsize % page_size != 0) {
        fprintf(stderr, "memsize must be a multiple of %ld\n", page_size);
        exit(EXIT_FAILURE);
    }

    int fd = open(filename, O_RDWR);
    if (fd == -1) {
        perror("open");
        exit(EXIT_FAILURE);
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        close(fd);
        exit(EXIT_FAILURE);
    }

    size_t total_size = st.st_size;
    uint64_t records;
    pread(fd, &records, sizeof(records), 0);

    size_t offset = 0;
    while (offset < total_size) {
        offset = (offset + page_size - 1) / page_size * page_size;

        size_t map_size = (total_size - offset) < memsize ? (total_size - offset) : memsize;

        if (offset + map_size > total_size) {
            map_size = total_size - offset;  
        }

        char *buffer = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset) + (int)sizeof(uint64_t);
        if (buffer == MAP_FAILED) {
            perror("mmap");
            close(fd);
            exit(EXIT_FAILURE);
        }

        size_t chunk_size = memsize / blocks;
        if (chunk_size % sizeof(index_s) != 0) {
            chunk_size = (chunk_size / sizeof(index_s)) * sizeof(index_s); 
        }
        if (chunk_size % sizeof(index_s) != 0) {
            fprintf(stderr, "chunk_size must be a multiple of sizeof(index_s)\n");
            exit(EXIT_FAILURE);
        }

        pthread_barrier_t barrier;
        pthread_mutex_t map_mutex;
        pthread_barrier_init(&barrier, NULL, threads);
        pthread_mutex_init(&map_mutex, NULL);

        int num_blocks = map_size / chunk_size;
        size_t block_size = chunk_size;

        int *block_map = calloc(num_blocks, sizeof(int));
        pthread_t threads_arr[threads];
        thread_params_t params[threads];

        int current_merge_pairs = 0;
        size_t merge_block_size = block_size; 
        int merge_blocks_count = num_blocks;
        int processed_blocks = 0; 

        for (int i = 0; i < threads; ++i) {
            params[i] = (thread_params_t){
                .thread_id = i,
                .buffer = buffer, 
                .block_size = block_size,
                .num_blocks = num_blocks,
                .block_map = block_map,
                .processed_blocks = &processed_blocks,
                .current_merge_pairs = &current_merge_pairs,
                .merge_block_size = &merge_block_size,
                .merge_blocks_count = &merge_blocks_count,
                .barrier = &barrier,
                .map_mutex = &map_mutex
            };
            pthread_create(&threads_arr[i], NULL, thread_func, &params[i]);
        }

        for (int i = 0; i < threads; ++i) {
            pthread_join(threads_arr[i], NULL);
        }

        free(block_map);
        munmap(buffer - sizeof(uint64_t), map_size);
        pthread_barrier_destroy(&barrier);
        pthread_mutex_destroy(&map_mutex);

        offset += map_size;
    }

    close(fd);
    return 0;
}
