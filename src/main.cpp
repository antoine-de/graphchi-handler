
#define GRAPHCHI_DISABLE_COMPRESSION


#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"
#include "importer.hpp"

#define THRESHOLD 1e-1    
#define RANDOMRESETPROB 0.15


typedef float VertexDataType;
typedef float EdgeDataType;

using graphchi::GraphChiProgram;
using graphchi::graphchi_context;
using graphchi::graphchi_vertex;
using graphchi::graphchi_edge;
using graphchi::get_option_string;
using graphchi::get_option_int;
using graphchi::graphchi_engine;
using graphchi::vertex_value;
using graphchi::vid_t;

struct PagerankProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    
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
        float sum=0;
        if (ginfo.iteration == 0) {
            /* On first iteration, initialize vertex and out-edges. 
               The initialization is important,
               because on every run, GraphChi will modify the data in the edges on disk. 
             */
            for(int i=0; i < v.num_outedges(); i++) {
                graphchi_edge<float> * edge = v.outedge(i);
                edge->set_data(1.0 / v.num_outedges());
            }
            v.set_data(RANDOMRESETPROB); 
        } else {
            /* Compute the sum of neighbors' weighted pageranks by
               reading from the in-edges. */
            for(int i=0; i < v.num_inedges(); i++) {
                float val = v.inedge(i)->get_data();
                sum += val;                    
            }
            
            /* Compute my pagerank */
            float pagerank = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
            
            /* Write my pagerank divided by the number of out-edges to
               each of my out-edges. */
            if (v.num_outedges() > 0) {
                float pagerankcont = pagerank / v.num_outedges();
                for(int i=0; i < v.num_outedges(); i++) {
                    graphchi_edge<float> * edge = v.outedge(i);
                    edge->set_data(pagerankcont);
                }
            }
                
            /* Keep track of the progression of the computation.
               GraphChi engine writes a file filename.deltalog. */
            ginfo.log_change(std::abs(pagerank - v.get_data()));
            
            /* Set my new pagerank as the vertex value */
            v.set_data(pagerank); 
        }
    }
};

int main(int argc, const char ** argv) {
    graphchi::graphchi_init(argc, argv);
    graphchi::metrics m("pagerank");
    global_logger().set_log_level(LOG_DEBUG);

    /* Parameters */
    int niters              = get_option_int("niters", 4);
    bool scheduler          = false;                    // Non-dynamic version of pagerank.
    int ntop                = get_option_int("top", 20);
    
    /* Process input file - if not already preprocessed */
    int nshards             = fetch_data(get_option_string("nshards", "auto"),
                                                get_option_string("worker_db"),
                                                get_option_string("link_db"));

    /* Run */
    graphchi::graphchi_engine<float, float> engine(FILE_NAME, nshards, scheduler, m); 
    engine.set_modifies_inedges(false); // Improves I/O performance.
   
    PagerankProgram program;
    engine.run(program, niters);
    
    /* Output top ranked vertices */
    std::vector< vertex_value<float> > top = graphchi::get_top_vertices<float>(FILE_NAME, ntop);
    std::cout << "Print top " << ntop << " vertices:" << std::endl;
    for(int i=0; i < (int)top.size(); i++) {
        std::cout << (i+1) << ". " << top[i].vertex << "\t" << top[i].value << std::endl;
    }
    
    metrics_report(m);    
    return 0;
}
