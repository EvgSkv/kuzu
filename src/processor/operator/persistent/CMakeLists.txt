add_subdirectory(reader)
add_subdirectory(writer/parquet)

add_library(kuzu_processor_operator_persistent
        OBJECT
        batch_insert.cpp
        node_batch_insert.cpp
        copy_rdf.cpp
        rel_batch_insert.cpp
        copy_to.cpp
        copy_to_csv.cpp
        copy_to_parquet.cpp
        delete.cpp
        delete_executor.cpp
        index_builder.cpp
        insert.cpp
        insert_executor.cpp
        merge.cpp
        set.cpp
        set_executor.cpp
        export_db.cpp
        import_db.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_persistent>
        PARENT_SCOPE)
