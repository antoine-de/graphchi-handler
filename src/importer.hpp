
#pragma once

#include <graphchi_basic_includes.hpp>
#include <preprocessing/sharder.hpp>
#include <stxxl/map>
#include <pqxx/pqxx>

#define DATA_NODE_BLOCK_SIZE (4096)
#define DATA_LEAF_BLOCK_SIZE (4096)

const std::string FILE_NAME = "graphchi";

using uuid_t = std::pair<uint64_t, uint64_t>;

namespace std {
    std::ostream& operator<<(std::ostream& stream, const uuid_t& val) {
        stream << val.first << "," << val.second;
        return stream;
    }
}

    struct VerticeInitData {
        float trust_rank;
        float porn_rank;
        float spam_rank;
    };

    struct Compare {
        bool operator () (const uuid_t& a, const uuid_t& b) const { return a < b; }
        static uuid_t max_value() { return {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()}; }
    };

    using VerticesIdMap = stxxl::map<uuid_t, uint64_t, Compare, DATA_NODE_BLOCK_SIZE, DATA_LEAF_BLOCK_SIZE>;
    using VerticesData = stxxl::VECTOR_GENERATOR<VerticeInitData>::result;
    using EdgeDataType = float; // for the moment it's a float, but it might become a more complex POD

    uint64_t fetch_vertices(const std::string& worker_db_cnx, VerticesIdMap& id_map, VerticesData& vertices_data) {
        pqxx::connection c(worker_db_cnx);
        pqxx::work transaction(c);

        auto results = transaction.exec("SELECT * FROM scores");
        uint64_t num_vertices = 0;
        for (const auto& row: results) {
            const uuid_t id = {row["hashl"].as<uint64_t>(), row["hashr"].as<uint64_t>()};
            id_map[id] = num_vertices++;

            auto get_nullable = [](const pqxx::result::field& val) -> float {
                if (val.is_null()) {
                    return {};
                }
                return val.as<float>();
            };

            vertices_data.push_back(VerticeInitData{
                get_nullable(row["trustrank"]),
                get_nullable(row["pornrank"]),
                get_nullable(row["spamrank"])
            });
            std::cout << " for id " << id << " idx = " << num_vertices - 1 << std::endl;
        }
        return num_vertices;
    }

    void fetch_edges(const VerticesIdMap& id_map, const std::string& link_db_cnx, graphchi::sharder<EdgeDataType>& sharder) {

        // sharder.preprocessing_add_edge(from, to);
    }

    int fetch_edges_and_shard(const VerticesIdMap& id_map, 
    const std::string& link_db_cnx, 
    const uint64_t num_vertices, 
    const std::string& nshards_string) {
        graphchi::sharder<EdgeDataType> sharder(FILE_NAME);
        sharder.start_preprocessing();

        fetch_edges(id_map, link_db_cnx, sharder);

        sharder.end_preprocessing();

        sharder.set_max_vertex_id(num_vertices);
        
        int nshards = sharder.execute_sharding(nshards_string);
        logstream(LOG_INFO) << "Successfully finished sharding " << std::endl;
        logstream(LOG_INFO) << "Created " << nshards << " shards." << std::endl;
        return nshards;
    }

    int fetch_data(const std::string& nshards_string, 
                    const std::string& worker_db_cnx, 
                    const std::string& link_db_cnx) {
        // for the moment we always create new shards
        logstream(LOG_INFO) << "create sharding now..." << std::endl;

        // we get all the vertices (websites) from the database
        const auto node_cache_size = VerticesIdMap::node_block_type::raw_size * 10;
        const auto leaf_cache_size = VerticesIdMap::leaf_block_type::raw_size * 10;
        VerticesIdMap id_map(node_cache_size, leaf_cache_size);
        VerticesData vertices_data{}; // TODO compute vertices size before
        const auto num_vertices = fetch_vertices(worker_db_cnx, id_map, vertices_data);

        // we get all the edges from the link database and use the id_map to set their id
        const int nshards = fetch_edges_and_shard(id_map, link_db_cnx, num_vertices, nshards_string);
        return nshards;
    }