
#define GRAPHCHI_DISABLE_COMPRESSION


#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"
#include "importer.hpp"

#define THRESHOLD 1e-1    
#define RANDOMRESETPROB 0.15


using graphchi::GraphChiProgram;
using graphchi::graphchi_context;
using graphchi::graphchi_vertex;
using graphchi::graphchi_edge;
using graphchi::get_option_string;
using graphchi::get_option_int;
using graphchi::graphchi_engine;
using graphchi::vertex_value;
using graphchi::vid_t;

typedef float EdgeDataType;

struct PagerankProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    VerticesData& vertices_data;
    PagerankProgram(VerticesData& d): vertices_data(d) {}
    /**
      * Called before an iteration starts. Not implemented.
      */
    void before_iteration(int iteration, graphchi_context &info) {
    }
    
    /**
      * Called after an iteration has finished. Not implemented.
      */
    void after_iteration(int iteration, graphchi_context &ginfo) {
    }
    
    /**
      * Called before an execution interval is started. Not implemented.
      */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &ginfo) {        
    }
    
    
    /**
      * Pagerank update function.
      */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &v, graphchi_context &ginfo) {
        if (ginfo.iteration == 0) {
            /* On first iteration, initialize vertex and out-edges. 
               The initialization is important,
               because on every run, GraphChi will modify the data in the edges on disk. 
             */
            if (v.outc > 0) {
                vertices_data[v.id()].page_rank = 1.0f / v.outc;
            }
        } else {
            float sum_page_rank = 0;
            float sum_porn_rank = 0;
            float sum_trust_rank = 0;
            for (int i = 0; i < v.num_inedges(); i++) {
                const auto& in_vertice_data = vertices_data[v.inedge(i)->vertexid];
                sum_page_rank += in_vertice_data.page_rank;
                sum_porn_rank += in_vertice_data.porn_rank;
                sum_trust_rank += in_vertice_data.trust_rank;
            }

            const auto& vertice_data = vertices_data[v.id()];
            float page_rank;
            if (v.outc > 0) {
                page_rank = (RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum_page_rank) / v.outc;
            } else {
                page_rank = (RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum_page_rank);
            }

            float porn_rank = vertice_data.porn_rank + sum_porn_rank / 10; // dumb value for the moment
            float trust_rank = vertice_data.trust_rank + sum_trust_rank / 10; // dumb value for the moment
            
            vertices_data[v.id()] = VertexDataType {
                page_rank,
                trust_rank,
                porn_rank,
                //spam_rank
            };

            if (ginfo.iteration == ginfo.num_iterations - 1) { // NOTE: I think we can skip this part and use directly vertices_data as output
                /* On last iteration, multiply pr by degree and store the result */
                auto& v_data = vertices_data[v.id()];
                if (v.outc) {
                    v_data.page_rank *= v.outc;
                }
                v.set_data(v_data); 
            }
        }
    }
};

void publish_results(const Context& ctx, const std::string& filename, graphchi::metrics& m) {
    graphchi::stripedio iomgr(m);
    vid_t readwindow = 1024 * 1024;
    size_t numvertices = graphchi::get_num_vertices(filename);
    auto vertexdata = graphchi::vertex_data_store<VertexDataType>{filename, numvertices, &iomgr};

    /* Iterate the vertex values and maintain the top-list */
    size_t idx = 0;
    vid_t st = 0;
    vid_t en = numvertices - 1;

    size_t count = 0;
    for (vid_t it_vertices = 0; it_vertices < numvertices; it_vertices += readwindow) {
        vid_t end = std::min(it_vertices + readwindow, numvertices);

        vertexdata.load(it_vertices, end - 1);
        for (vid_t v = it_vertices; v < end; v++) {
            const VertexDataType& val = *vertexdata.vertex_data_ptr(v);
            const auto& uuid = ctx.vertices_uuid[v];

            std::cout << " for graphchi id " << v << " uuid " << uuid << " value = " << val << std::endl;
        }
    }
}

int main(int argc, const char ** argv) {
    graphchi::graphchi_init(argc, argv);
    graphchi::metrics m("pagerank");
    global_logger().set_log_level(LOG_DEBUG);

    /* Parameters */
    int niters              = get_option_int("niters", 4);
    bool scheduler          = false;                    // Non-dynamic version of pagerank.
    int ntop                = get_option_int("top", 20);
    
    /* Process input file - if not already preprocessed */
    Context ctx;
    int nshards             = fetch_data(ctx, get_option_string("nshards", "auto"),
                                                get_option_string("worker_db"),
                                                get_option_string("link_db"));

    /* Run */
    graphchi::graphchi_engine<VertexDataType, EdgeDataType> engine(FILE_NAME, nshards, scheduler, m); 
    engine.set_modifies_inedges(false); // Improves I/O performance.
   
    PagerankProgram program(ctx.vertices_data);
    engine.run(program, niters);
    
    publish_results(ctx, FILE_NAME, m);
    
    metrics_report(m);    
    return 0;
}
