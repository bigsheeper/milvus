#include "SegmentBase.h"
#include "SegmentDefs.h"
#include "cwrap.h"

CSegmentBase
SegmentBaseInit() {
  CSegmentBase seg = milvus::dog_segment::CreateSegment();
  return (void*)seg;
}

int32_t Insert(CSegmentBase c_segment, signed long int size, const unsigned int* primary_keys, const unsigned long int* timestamps, DogDataChunk values) {
  auto segment = (milvus::dog_segment::SegmentBase*)c_segment;
  milvus::dog_segment::DogDataChunk dataChunk{};

  dataChunk.raw_data = values.raw_data;
  dataChunk.sizeof_per_row = values.sizeof_per_row;
  dataChunk.count = values.count;

  auto res = segment->Insert(size, primary_keys, timestamps, dataChunk);
  return res.code();
}

