#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>

typedef struct {
    double time_mark;
    uint64_t recno;
} index_s;

typedef struct {
    uint64_t records;
    index_s idx[];
} index_hdr_s;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s filename\n", argv[0]);
        return 1;
    }

    FILE *file = fopen(argv[1], "rb");
    if (!file) {
        perror("fopen");
        return 1;
    }

    index_hdr_s hdr;
    if (fread(&hdr.records, sizeof(hdr.records), 1, file) != 1) {
        perror("fread records");
        fclose(file);
        return 1;
    }

    printf("Total records: %" PRIu64 "\n", hdr.records);

    // Чтение записей напрямую из файла
    for (uint64_t i = 0; i < hdr.records; ++i) {
        index_s record;
        if (fread(&record, sizeof(record), 1, file) != 1) {
            perror("fread record");
            fclose(file);
            return 1;
        }
        if (record.recno > 100000) {
            printf("Record %" PRIu64 ": time=%.5f, recno=228" "\n", i + 1, record.time_mark);
        }
        else {
            printf("Record %" PRIu64 ": time=%.5f, recno=%" PRIu64 "\n", i + 1, record.time_mark, record.recno);
        }
    }

    fclose(file);
    return 0;
}

