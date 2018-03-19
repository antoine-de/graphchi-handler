#pragma once
#include <stxxl/map>

#define DATA_NODE_BLOCK_SIZE (4096)
#define DATA_LEAF_BLOCK_SIZE (4096)

using uuid_t = std::array<uint64_t, 2>;

namespace std {
    inline std::ostream& operator<<(std::ostream& stream, const uuid_t& val) {
        stream << val[0] << "-" << val[1];
        return stream;
    }
}


struct VertexDataType {
    float page_rank;
    float trust_rank;
    float porn_rank;
    // float spam_rank;
};

inline std::ostream& operator<<(std::ostream& stream, const VertexDataType& val) {
    stream << "pr: " << val.page_rank 
    << ", trust: " << val.trust_rank 
    << ", porn: " << val.porn_rank;
    return stream;
}

struct MapIdCompare {
    bool operator () (const uuid_t& a, const uuid_t& b) const { return a < b; }
    static uuid_t max_value() { return {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()}; }
};

using VerticesIdMap = stxxl::map<uuid_t, uint64_t, MapIdCompare, DATA_NODE_BLOCK_SIZE, DATA_LEAF_BLOCK_SIZE>;
using VerticesData = stxxl::VECTOR_GENERATOR<VertexDataType>::result;
using VerticesUuid = stxxl::VECTOR_GENERATOR<uuid_t>::result;
using EdgeDataType = float; // for the moment it's a float, but it might become a more complex POD

struct Context {
    Context(const uint64_t map_node_cache_size = VerticesIdMap::node_block_type::raw_size * 10, 
            const uint64_t map_leaf_cache_size = VerticesIdMap::leaf_block_type::raw_size * 10): 
            id_map(map_node_cache_size, map_leaf_cache_size) {}

    VerticesIdMap id_map; // used to associate an uuid to an internal graphchi ID
    VerticesData vertices_data{}; // used to initialize the graph
    VerticesUuid vertices_uuid{}; // used to find the original uuid at the end of the run
};