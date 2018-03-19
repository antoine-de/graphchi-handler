
#pragma once

#include <graphchi_basic_includes.hpp>
#include <preprocessing/sharder.hpp>
#include <pqxx/pqxx>
#include "deps/MurmurHash3.h"
#include "objects.hpp"

const std::string FILE_NAME = "graphchi";

uuid_t mm3(const std::string& val) {
    uuid_t hash;
    const auto default_seed = 1717;
    MurmurHash3_x64_128(val.c_str(), val.size(), default_seed, &hash);
    return hash;
}

uuid_t read_id(const pqxx::tuple& row) {
    // for the moment there is no uri in the db, so we compute the hash again, but it's temporary, we should always use the central ID
    
    // return {row["hashl"].as<uint64_t>(), row["hashr"].as<uint64_t>()};
    const std::string url = row["url"].as<std::string>();
    return mm3(url);
}

uint64_t fetch_vertices(Context& ctx, const std::string& worker_db_cnx) {
    pqxx::connection c(worker_db_cnx);
    pqxx::work transaction(c);

    auto results = transaction.exec("SELECT * FROM scores");
    uint64_t num_vertices = 0;
    for (const auto& row: results) {
        const uuid_t id = read_id(row);
        ctx.id_map[id] = num_vertices;
        ctx.vertices_uuid.push_back(id);

        const auto get_nullable = [](const pqxx::result::field& val) -> float {
            if (val.is_null()) {
                return {};
            }
            return val.as<float>();
        };

        ctx.vertices_data.push_back(VertexDataType{
            0,
            get_nullable(row["trustrank"]),
            get_nullable(row["pornrank"]),
            // get_nullable(row["spamrank"])
        });
        std::cout << " for url " << row["url"].as<std::string>().substr(6, 20) << " id " << id << " idx = " << num_vertices << " pron = " 
        << ctx.vertices_data.back().porn_rank << std::endl;
        num_vertices++;
    }
    return num_vertices;
}

void add_edge(const VerticesIdMap& id_map, graphchi::sharder<EdgeDataType>& sharder, const uuid_t& from, const uuid_t& to) {
    auto from_it = id_map.find(from);
    if (from_it == id_map.end()) {
        std::cerr << "impossible to find " << from << " source node, skipping edge" << std::endl;
        return;
    }
    const uint64_t from_idx = from_it->second;
    auto to_it = id_map.find(to);
    if (to_it == id_map.end()) {
        std::cerr << "impossible to find " << to << " target node, skipping edge" << std::endl;
        return;
    }
    const uint64_t to_idx = to_it->second;
    std::cout <<" add edge " << from_idx << " -> " << to_idx <<  std::endl;
    sharder.preprocessing_add_edge(from_idx, to_idx);
}

void fetch_edges(const VerticesIdMap& id_map, const std::string& link_db_cnx, graphchi::sharder<EdgeDataType>& sharder) {

    // for the moment we read a dump edge file
    std::ifstream file{link_db_cnx, std::fstream::in};
    std::string line;
    while(std::getline(file, line)) {
        std::stringstream lineStream(line);
        std::string from_url, to_url;
        assert(std::getline(lineStream, from_url, ','));
        assert(std::getline(lineStream, to_url, ','));

        const uuid_t from = mm3(from_url);
        const uuid_t to = mm3(to_url);
        std::cout << from_url << " (" << from << ") -> " << to_url << " (" << to << ")" << std::endl;

        add_edge(id_map, sharder, from, to);
    }
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

int fetch_data(Context& ctx, const std::string& nshards_string, 
                const std::string& worker_db_cnx, 
                const std::string& link_db_cnx) {
    // for the moment we always create new shards
    logstream(LOG_INFO) << "create sharding now..." << std::endl;

    // we get all the vertices (websites) from the database
    const auto num_vertices = fetch_vertices(ctx, worker_db_cnx);

    // we get all the edges from the link database and use the id_map to set their id
    const int nshards = fetch_edges_and_shard(ctx.id_map, link_db_cnx, num_vertices, nshards_string);
    return nshards;
}
