#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdbool.h>
#include <float.h>

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
    size_t buffer_size;   
    size_t block_size;
    int num_blocks;
    int *block_map;
    int *processed_blocks;
    int *current_merge_pairs;
    size_t *merge_block_size;
    int *merge_blocks_count;
    pthread_barrier_t *barrier;
    pthread_mutex_t *map_mutex;
    int *cancel_flag;
    pthread_cond_t *cancel_cond;
} thread_params_t;

int compare_index(const void *a, const void *b);

void merge_blocks(index_s *block1, index_s *block2, size_t block1_size, size_t block2_size);

void merge_file_parts(const char *filename, size_t total_parts, size_t part_size, size_t total_records);

int get_next_block(thread_params_t *params);

void *thread_func(void *arg);

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

    size_t header_size = sizeof(uint64_t);
    size_t record_size = sizeof(index_s);
    size_t total_parts = 0;
    size_t offset = 0;

    size_t chunk_size = memsize / blocks;
    chunk_size = (chunk_size / record_size) * record_size;
    if (chunk_size == 0) chunk_size = record_size;
    size_t part_size = chunk_size;

    pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cancel_cond = PTHREAD_COND_INITIALIZER;
    int cancel_flag = 0;

    while (offset < total_size) {
        size_t map_size = (total_size - offset) < memsize ? (total_size - offset) : memsize;
        if (offset + map_size > total_size) {
            map_size = total_size - offset;
        }

        char *buffer;
        size_t effective_size = map_size;
        
        if (offset == 0) {
            buffer = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
            if (buffer == MAP_FAILED) {
                perror("mmap");
                close(fd);
                exit(EXIT_FAILURE);
            }
            buffer += header_size;
            effective_size -= header_size;
        } else {
            buffer = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
            if (buffer == MAP_FAILED) {
                perror("mmap");
                close(fd);
                exit(EXIT_FAILURE);
            }
        }

        size_t chunk_size = part_size;

        pthread_barrier_t barrier;
        pthread_mutex_t map_mutex;
        pthread_barrier_init(&barrier, NULL, threads);
        pthread_mutex_init(&map_mutex, NULL);

        int num_blocks = effective_size / chunk_size;
        if (effective_size % chunk_size != 0) num_blocks++;
        total_parts += num_blocks;

        int *block_map = calloc(num_blocks, sizeof(int));
        pthread_t *threads_arr = calloc(threads, sizeof(pthread_t));
        thread_params_t *params = calloc(threads, sizeof(thread_params_t));

        int current_merge_pairs = 0;
        size_t merge_block_size = chunk_size;
        int merge_blocks_count = num_blocks;

        for (int i = 0; i < threads; ++i) {
            params[i] = (thread_params_t){
                .thread_id = i,
                .buffer = buffer,
                .buffer_size = effective_size,
                .block_size = chunk_size,
                .num_blocks = num_blocks,
                .block_map = block_map,
                .current_merge_pairs = &current_merge_pairs,
                .merge_block_size = &merge_block_size,
                .merge_blocks_count = &merge_blocks_count,
                .barrier = &barrier,
                .map_mutex = &map_mutex,
                .cancel_flag = &cancel_flag,
                .cancel_cond = &cancel_cond
            };
            pthread_create(&threads_arr[i], NULL, thread_func, &params[i]);
        }

        for (int i = 0; i < threads; ++i) pthread_join(threads_arr[i], NULL);

        if (offset + map_size >= total_size) {
            pthread_mutex_lock(&global_mutex);
            cancel_flag = 1;
            pthread_cond_broadcast(&cancel_cond);
            pthread_mutex_unlock(&global_mutex);
        }

        free(block_map);
        free(threads_arr);
        free(params);

        if (offset == 0) munmap(buffer - header_size, map_size);
        else munmap(buffer, map_size);

        pthread_barrier_destroy(&barrier);
        pthread_mutex_destroy(&map_mutex);

        offset += map_size;
    }

    merge_file_parts(filename, total_parts, part_size, records);

    if (records > 0) {
        int fd = open(filename, O_RDWR);
        if (fd != -1) {
            size_t total_data_size = sizeof(uint64_t) + records * sizeof(index_s);
            char *data = mmap(NULL, total_data_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (data != MAP_FAILED) {
                index_hdr_s *header = (index_hdr_s *)data;
                qsort(header->idx, records, sizeof(index_s), compare_index);
                msync(data, total_data_size, MS_SYNC);
                munmap(data, total_data_size);
            }
            close(fd);
        }
    }

    pthread_mutex_destroy(&global_mutex);
    pthread_cond_destroy(&cancel_cond);
    close(fd);
    return 0;
}

int compare_index(const void *a, const void *b) {
    const index_s *ia = (const index_s *)a;
    const index_s *ib = (const index_s *)b;
    
    if (ia->time_mark < ib->time_mark) return -1;
    if (ia->time_mark > ib->time_mark) return 1;
    
    if (ia->recno < ib->recno) return -1;
    if (ia->recno > ib->recno) return 1;
    
    return 0;
}

void merge_blocks(index_s *block1, index_s *block2, size_t block1_size, size_t block2_size) {
    size_t total_size = block1_size + block2_size;
    index_s *merged = malloc(total_size * sizeof(index_s));
    if (!merged) {
        perror("malloc in merge_blocks");
        exit(EXIT_FAILURE);
    }
    
    size_t i = 0, j = 0, k = 0;
    
    while (i < block1_size && j < block2_size) {
        if (compare_index(&block1[i], &block2[j]) <= 0)
            merged[k++] = block1[i++];
        else
            merged[k++] = block2[j++];
    }
    
    while (i < block1_size) merged[k++] = block1[i++];
    while (j < block2_size) merged[k++] = block2[j++];
    
    memcpy(block1, merged, total_size * sizeof(index_s));
    free(merged);
}

void merge_file_parts(const char *filename, size_t total_parts, size_t part_size, size_t total_records) {
    int fd = open(filename, O_RDWR);
    if (fd == -1) {
        perror("open in merge_file_parts");
        exit(EXIT_FAILURE);
    }

    size_t header_size = sizeof(uint64_t);
    size_t total_size = header_size + total_records * sizeof(index_s);
    
    char *mapped_data = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_data == MAP_FAILED) {
        perror("mmap in merge_file_parts");
        close(fd);
        exit(EXIT_FAILURE);
    }

    index_hdr_s *file_header = (index_hdr_s *)mapped_data;
    
    typedef struct {
        index_s record;
        size_t part_idx;
        size_t pos;
        size_t max_pos;
    } heap_entry;

    size_t records_per_part = part_size / sizeof(index_s);
    if (records_per_part == 0) records_per_part = 1;
    
    heap_entry *heap = malloc(total_parts * sizeof(heap_entry));
    if (!heap) {
        perror("malloc failed for heap");
        munmap(mapped_data, total_size);
        close(fd);
        exit(EXIT_FAILURE);
    }
    
    index_s *result = malloc(total_records * sizeof(index_s));
    if (!result) {
        perror("malloc failed for result");
        free(heap);
        munmap(mapped_data, total_size);
        close(fd);
        exit(EXIT_FAILURE);
    }
    
    size_t heap_size = 0;
    
    for (size_t i = 0; i < total_parts && heap_size < total_parts; i++) {
        size_t start_record = i * records_per_part;
        if (start_record >= total_records) 
            continue;
            
        size_t max_records = records_per_part;
        if (start_record + max_records > total_records)
            max_records = total_records - start_record;
            
        if (max_records > 0) {
            heap[heap_size].record = file_header->idx[start_record];
            heap[heap_size].part_idx = i;
            heap[heap_size].pos = 0;
            heap[heap_size].max_pos = max_records;
            heap_size++;
        }
    }
    
    for (size_t i = heap_size / 2; i > 0; i--) {
        size_t parent = i - 1;
        heap_entry temp = heap[parent];
        size_t child;
        
        for (child = 2 * parent + 1; child < heap_size; child = 2 * parent + 1) {
            if (child + 1 < heap_size && 
                compare_index(&heap[child + 1].record, &heap[child].record) < 0)
                child++;
                
            if (compare_index(&temp.record, &heap[child].record) <= 0)
                break;
                
            heap[parent] = heap[child];
            parent = child;
        }
        
        heap[parent] = temp;
    }
    
    size_t result_idx = 0;
    
    while (heap_size > 0 && result_idx < total_records) {
        result[result_idx++] = heap[0].record;
        
        size_t part_idx = heap[0].part_idx;
        size_t next_pos = heap[0].pos + 1;
        size_t start_record = part_idx * records_per_part;
        
        if (next_pos < heap[0].max_pos) {
            heap[0].record = file_header->idx[start_record + next_pos];
            heap[0].pos = next_pos;
            
            size_t parent = 0;
            heap_entry temp = heap[0];
            size_t child;
            
            for (child = 1; child < heap_size; child = 2 * parent + 1) {
                if (child + 1 < heap_size && 
                    compare_index(&heap[child + 1].record, &heap[child].record) < 0)
                    child++;
                    
                if (compare_index(&temp.record, &heap[child].record) <= 0)
                    break;
                    
                heap[parent] = heap[child];
                parent = child;
            }
            
            heap[parent] = temp;
        } else {
            heap[0] = heap[--heap_size];
            
            if (heap_size > 0) {
                size_t parent = 0;
                heap_entry temp = heap[0];
                size_t child;
                
                for (child = 1; child < heap_size; child = 2 * parent + 1) {
                    if (child + 1 < heap_size && 
                        compare_index(&heap[child + 1].record, &heap[child].record) < 0)
                        child++;
                        
                    if (compare_index(&temp.record, &heap[child].record) <= 0)
                        break;
                        
                    heap[parent] = heap[child];
                    parent = child;
                }
                
                heap[parent] = temp;
            }
        }
    }
    
    if (result_idx == total_records - 1 && total_records > 0) {
        index_s last_record = file_header->idx[total_records - 1];
        size_t correct_pos = 0;
        
        while (correct_pos < result_idx && 
               compare_index(&last_record, &result[correct_pos]) > 0) {
            correct_pos++;
        }
        
        if (correct_pos < result_idx) {
            memmove(&result[correct_pos + 1], &result[correct_pos], 
                   (result_idx - correct_pos) * sizeof(index_s));
        }
        
        result[correct_pos] = last_record;
        result_idx++;
    }
    
    memcpy(file_header->idx, result, total_records * sizeof(index_s));
    
    qsort(file_header->idx, total_records, sizeof(index_s), compare_index);
    
    free(result);
    free(heap);
    munmap(mapped_data, total_size);
    close(fd);
}

int get_next_block(thread_params_t *params) {
    pthread_mutex_lock(params->map_mutex);
    for (int i = 0; i < params->num_blocks; ++i) {
        if (!params->block_map[i]) {
            params->block_map[i] = 1;
            pthread_mutex_unlock(params->map_mutex);
            return i;
        }
    }
    pthread_mutex_unlock(params->map_mutex);
    return -1;
}

void *thread_func(void *arg) {
    thread_params_t *params = (thread_params_t *)arg;
    pthread_barrier_wait(params->barrier);

    int current_block;
    while ((current_block = get_next_block(params)) != -1) {
        size_t offset = current_block * params->block_size;
        if (offset >= params->buffer_size) 
            break;
        
        size_t remaining = params->buffer_size - offset;
        size_t block_size = (params->block_size < remaining) ? params->block_size : remaining;
        size_t num_records = block_size / sizeof(index_s);
        
        if (num_records == 0) 
            continue;
        
        index_s *block = (index_s *)(params->buffer + offset);
        
        qsort(block, num_records, sizeof(index_s), compare_index);
        
        if (offset + block_size >= params->buffer_size) {
            size_t last_record_index = num_records - 1;
            if (last_record_index < num_records) {
                index_s last_record = block[last_record_index];
                size_t correct_pos = 0;
                
                while (correct_pos < last_record_index && 
                       compare_index(&last_record, &block[correct_pos]) > 0) {
                    correct_pos++;
                }
                
                if (correct_pos < last_record_index) {
                    memmove(&block[correct_pos + 1], &block[correct_pos], 
                           (last_record_index - correct_pos) * sizeof(index_s));
                    block[correct_pos] = last_record;
                }
            }
        }
    }

    pthread_barrier_wait(params->barrier);

    while (*(params->merge_blocks_count) > 1) {
        pthread_mutex_lock(params->map_mutex);
        int pairs = *(params->merge_blocks_count) / 2;
        int my_pair = (*params->current_merge_pairs) < pairs ? (*params->current_merge_pairs)++ : -1;
        pthread_mutex_unlock(params->map_mutex);

        if (my_pair == -1) 
            break;

        size_t merge_size = *params->merge_block_size;
        size_t offset1 = my_pair * 2 * merge_size;
        size_t offset2 = offset1 + merge_size;

        if (offset1 >= params->buffer_size) 
            continue;
        
        size_t size1 = merge_size;
        if (offset1 + size1 > params->buffer_size) 
            size1 = params->buffer_size - offset1;
            
        size_t size2 = merge_size;
        if (offset2 >= params->buffer_size) {
            size2 = 0;
        } else if (offset2 + size2 > params->buffer_size) {
            size2 = params->buffer_size - offset2;
        }
        
        size_t records1 = size1 / sizeof(index_s);
        size_t records2 = size2 / sizeof(index_s);
        
        if (records1 == 0 || records2 == 0) 
            continue;

        index_s *b1 = (index_s *)(params->buffer + offset1);
        index_s *b2 = (index_s *)(params->buffer + offset2);
        
        merge_blocks(b1, b2, records1, records2);
        
        if (offset1 + size1 + size2 >= params->buffer_size) {
            size_t merged_size = records1 + records2;
            if (merged_size > 0) {
                qsort(b1, merged_size, sizeof(index_s), compare_index);
            }
        }
    }

    pthread_barrier_wait(params->barrier);
    return NULL;
}
