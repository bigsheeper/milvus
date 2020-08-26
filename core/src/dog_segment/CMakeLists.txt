set(DOG_SEGMENT_FILES
        SegmentNaive.cpp
        cwrap.cpp
        )
# Third Party dablooms file
#aux_source_directory( ${MILVUS_THIRDPARTY_SRC}/dablooms  THIRDPARTY_DABLOOMS_FILES )
add_library( dog_segment STATIC )
target_sources( dog_segment PRIVATE
        ${DOG_SEGMENT_FILES}
        #${THIRDPARTY_DABLOOMS_FILES}
        )
#add_dependencies( segment sqlite mysqlpp )
target_link_libraries(dog_segment tbb)

add_executable(main main.cpp)
target_link_libraries(main
        dog_segment
        server
        milvus_engine
        metrics
        config
        stdc++
        utils
        tracing
        query
        log)