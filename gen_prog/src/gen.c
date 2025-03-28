#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <math.h>
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h> 

typedef struct {
    double time_mark;
    uint64_t recno;
} index_s;

typedef struct {
    uint64_t records;
    index_s idx[];
} index_hdr_s;

static uint32_t get_yesterday_mjd() {
    time_t now = time(NULL);
    now -= 86400;
    struct tm tm_yesterday;
    localtime_r(&now, &tm_yesterday);

    int Y = tm_yesterday.tm_year + 1900;
    int M = tm_yesterday.tm_mon + 1;
    int D = tm_yesterday.tm_mday;

    int A = (14 - M) / 12;
    int B = Y + 4800 - A;
    int C = M + 12*A - 3;
    double JD = D + (153*C + 2)/5.0 + 365*B + B/4.0 - B/100.0 + B/400.0 - 32045;
    return (uint32_t)(JD - 2400000.5);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s num_records memsize filename\n", argv[0]);
        return 1;
    }

    uint64_t num_records = strtoull(argv[1], NULL, 10);
    size_t memsize = strtoull(argv[2], NULL, 10);
    const char *filename = argv[3];

    if ((num_records * sizeof(index_s)) % memsize != 0) {
        fprintf(stderr, "num_records * sizeof(index_s) must be a multiple of memsize\n");
        return 1;
    }

    if (num_records % 256 != 0) {
        fprintf(stderr, "Number of records must be a multiple of 256\n");
        return 1;
    }

    long page_size = sysconf(_SC_PAGESIZE);  
    if (page_size == -1) {
        perror("sysconf");
        return 1;
    }

    size_t data_size = num_records * sizeof(index_s);
    size_t data_padding = (memsize - (data_size % memsize)) % memsize;
    size_t total_data_size = data_size + data_padding;

    size_t total_size_without_file_padding = sizeof(uint64_t) + total_data_size;
    size_t file_padding = (page_size - (total_size_without_file_padding % page_size)) % page_size;
    size_t total_size = sizeof(uint64_t) + total_data_size + file_padding;

    index_hdr_s *hdr = malloc(total_size);
    if (!hdr) {
        perror("malloc");
        return 1;
    }
    memset(hdr, 0, total_size); 

    hdr->records = num_records;

    srand(time(NULL));
    uint32_t mjd_yesterday = get_yesterday_mjd();
    if (mjd_yesterday < 15020) {
        fprintf(stderr, "MJD yesterday is too small\n");
        free(hdr);
        return 1;
    }

    for (uint64_t i = 0; i < num_records; ++i) {
        hdr->idx[i].time_mark = 15020 + rand() % (mjd_yesterday - 15020 + 1) + (double)rand() / RAND_MAX;
        hdr->idx[i].recno = i + 1;
    }

    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("fopen");
        free(hdr);
        return 1;
    }

    if (fwrite(hdr, total_size, 1, file) != 1) {
        perror("fwrite");
        free(hdr);
        fclose(file);
        return 1;
    }

    free(hdr);
    fclose(file);
    return 0;
}

