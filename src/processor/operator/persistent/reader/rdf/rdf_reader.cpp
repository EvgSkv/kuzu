#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/vector/value_vector.h"
#include "processor/operator/persistent/reader/rdf/rdf_utils.h"
#include "serd.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

RdfReader::~RdfReader() {
    serd_reader_end_stream(reader);
    serd_reader_free(reader);
    serd_env_free(env);
    // Even if the close fails, the stream is in an undefined state. There really isn't anything we
    // can do.
    (void)fclose(fp);
}

static SerdSyntax getSerdSyntax(FileType fileType) {
    switch (fileType) {
    case FileType::TURTLE:
        return SerdSyntax ::SERD_TURTLE;
    case FileType::NQUADS:
        return SerdSyntax ::SERD_NQUADS;
    default:
        KU_UNREACHABLE;
    }
}

void RdfReader::initInternal(SerdStatementSink statementSink) {
    KU_ASSERT(reader == nullptr);
    fp = fopen(this->filePath.c_str(), "rb");
    reader = serd_reader_new(
        getSerdSyntax(fileType), this, nullptr, baseHandle, prefixHandle, statementSink, nullptr);
    serd_reader_set_error_sink(reader, errorHandle, this);
    auto fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
    env = serd_env_new(nullptr);
}

SerdStatus RdfReader::errorHandle(void*, const SerdError* error) {
    // TODO: we should probably have a strict mode where exceptions are being thrown.
    //        common::stringFormat("{} while reading rdf file at line {} and col {}",
    //                (char*)serd_strerror(error->status), error->line, error->col));
    return error->status;
}

SerdStatus RdfReader::baseHandle(void* handle, const SerdNode* baseNode) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    serd_env_set_base_uri(reader->env, baseNode);
    reader->hasBaseUri = true;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::prefixHandle(
    void* handle, const SerdNode* nameNode, const SerdNode* uriNode) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    serd_env_set_prefix(reader->env, nameNode, uriNode);
    return SERD_SUCCESS;
}

offset_t RdfReader::readChunk(DataChunk* dataChunk) {
    if (cursor == store_->size()) {
        cursor = 0;
        store_->clear();
        while (true) {
            if (status == SERD_FAILURE || store_->size() > DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            if (status > SERD_FAILURE) {
                serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            }
            status = serd_reader_read_chunk(reader);
        }
    }
    if (store_->isEmpty()) {
        return 0;
    }
    auto numTuplesRead = readToVector(dataChunk);
    cursor += numTuplesRead;
    dataChunk->state->selVector->selectedSize = numTuplesRead;
    return numTuplesRead;
}

void RdfReader::readAll() {
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (status == SERD_FAILURE) {
            return;
        }
        if (status > SERD_FAILURE) {
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
        }
    }
}

static void appendSerdChunk(std::string& str, const SerdChunk& chunk) {
    if (chunk.len != 0) {
        str += std::string((const char*)chunk.buf, chunk.len);
    }
}

void RdfReader::addNode(std::vector<std::string>& vector, const SerdNode* node) {
    switch (node->type) {
    case SERD_URI: {
        if (!hasBaseUri || serd_uri_string_has_scheme(node->buf)) {
            vector.emplace_back((const char*)node->buf, node->n_bytes);
            return;
        }
        SerdURI inputUri;
        serd_uri_parse(node->buf, &inputUri);
        SerdURI baseUri;
        serd_env_get_base_uri(env, &baseUri);
        SerdURI resultUri;
        serd_uri_resolve(&inputUri, &baseUri, &resultUri);
        std::string result;
        appendSerdChunk(result, resultUri.scheme);
        if (resultUri.scheme.len != 0) {
            result += "://";
        }
        appendSerdChunk(result, resultUri.authority);
        appendSerdChunk(result, resultUri.path_base);
        appendSerdChunk(result, resultUri.path);
        appendSerdChunk(result, resultUri.query);
        appendSerdChunk(result, resultUri.fragment);
        vector.push_back(std::move(result));
    } break;
    case SERD_CURIE: {
        SerdChunk prefix = SerdChunk();
        SerdChunk suffix = SerdChunk();
        auto st = serd_env_expand(env, node, &prefix, &suffix);
        if (st == SERD_SUCCESS) {
            auto expandedUri = std::string((const char*)prefix.buf, prefix.len) +
                               std::string((const char*)suffix.buf, suffix.len);
            vector.push_back(std::move(expandedUri));
        } else {
            vector.emplace_back((const char*)node->buf, node->n_bytes);
        }
    } break;
    default:
        vector.emplace_back((const char*)node->buf, node->n_bytes);
    }
}

SerdStatus RdfResourceReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, ResourceStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        reader->addNode(store.resources, subject);
        reader->addNode(store.resources, predicate);
    } else {
        reader->addNode(store.resources, subject);
        reader->addNode(store.resources, predicate);
        reader->addNode(store.resources, object);
    }
    return SERD_SUCCESS;
}

uint64_t RdfResourceReader::readToVector(DataChunk* dataChunk) {
    auto rVector = dataChunk->getValueVector(0).get();
    auto& store = ku_dynamic_cast<RdfStore&, ResourceStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(rVector, i, store.resources[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode*, const SerdNode*, const SerdNode* object, const SerdNode* object_datatype,
    const SerdNode* object_lang) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, LiteralStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        reader->addNode(store.literals, object);
        if (object_datatype != nullptr) {
            auto typeID = RdfUtils::getLogicalTypeID(
                (const char*)object_datatype->buf, object_datatype->n_bytes);
            store.literalTypes.push_back(typeID);
        } else {
            store.literalTypes.push_back(LogicalTypeID::STRING);
        }
        if (object_lang != nullptr) {
            reader->addNode(store.langs, object_lang);
        } else {
            store.langs.push_back("");
        }
    }
    return SERD_SUCCESS;
}

uint64_t RdfLiteralReader::readToVector(common::DataChunk* dataChunk) {
    auto lVector = dataChunk->getValueVector(0).get();
    auto langVector = dataChunk->getValueVector(1).get();
    auto& store = ku_dynamic_cast<RdfStore&, LiteralStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& literal = store.literals[cursor + i];
        auto& type = store.literalTypes[cursor + i];
        auto& lang = store.langs[cursor + i];
        RdfUtils::addRdfLiteral(lVector, i, literal, type);
        StringVector::addString(langVector, i, lang);
    }
    return numTuplesToScan;
}

SerdStatus RdfResourceTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, ResourceTripleStore&>(*reader->store_);
    if (object->type != SERD_LITERAL) {
        reader->addNode(store.subjects, subject);
        reader->addNode(store.predicates, predicate);
        reader->addNode(store.objects, object);
    }
    return SERD_SUCCESS;
}

uint64_t RdfResourceTripleReader::readToVector(DataChunk* dataChunk) {
    auto sVector = dataChunk->getValueVector(0).get();
    auto pVector = dataChunk->getValueVector(1).get();
    auto oVector = dataChunk->getValueVector(2).get();
    auto& store = ku_dynamic_cast<RdfStore&, ResourceTripleStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.subjects[cursor + i]);
        StringVector::addString(pVector, i, store.predicates[cursor + i]);
        StringVector::addString(oVector, i, store.objects[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, LiteralTripleStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        reader->addNode(store.subjects, subject);
        reader->addNode(store.predicates, predicate);
    }
    return SERD_SUCCESS;
}

uint64_t RdfLiteralTripleReader::readToVector(DataChunk* dataChunk) {
    auto sVector = dataChunk->getValueVector(0).get();
    auto pVector = dataChunk->getValueVector(1).get();
    auto oVector = dataChunk->getValueVector(2).get();
    auto& store = ku_dynamic_cast<RdfStore&, LiteralTripleStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.subjects[cursor + i]);
        StringVector::addString(pVector, i, store.predicates[cursor + i]);
        oVector->setValue(i, startOffset + numLiteralTriplesScanned + i);
    }
    numLiteralTriplesScanned += numTuplesToScan;
    return numTuplesToScan;
}

SerdStatus RdfTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode* object_lang) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        reader->addNode(store.ltStore.subjects, subject);
        reader->addNode(store.ltStore.predicates, predicate);
        reader->addNode(store.ltStore.objects, object);
        if (object_datatype != nullptr) {
            auto typeID = RdfUtils::getLogicalTypeID(
                (const char*)object_datatype->buf, object_datatype->n_bytes);
            store.ltStore.objectTypes.push_back(typeID);
        } else {
            store.ltStore.objectTypes.push_back(LogicalTypeID::STRING);
        }
        if (object_lang != nullptr) {
            reader->addNode(store.ltStore.langs, object_lang);
        } else {
            store.ltStore.langs.push_back("");
        }
    } else {
        reader->addNode(store.rtStore.subjects, subject);
        reader->addNode(store.rtStore.predicates, predicate);
        reader->addNode(store.rtStore.objects, object);
    }
    return SERD_SUCCESS;
}

} // namespace processor
} // namespace kuzu
