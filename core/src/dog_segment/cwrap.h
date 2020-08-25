#ifdef __cplusplus
extern "C" {
#endif

struct DogDataChunk {
    void* raw_data;      // schema
    int sizeof_per_row;  // alignment
    int64_t count;
};

typedef void* CSegmentBase;

CSegmentBase SegmentBaseInit();

int32_t Insert(CSegmentBase c_segment, signed long int size, const unsigned int* primary_keys, const unsigned long int* timestamps, DogDataChunk values);

#ifdef __cplusplus
}
#endif