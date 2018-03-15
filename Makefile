INCFLAGS = -I/usr/local/include/ -I./src/ -I../graphchi-cpp/src

CPP = g++
CPPFLAGS = --std=c++14 -Ofast -g -fno-signed-zeros -fno-trapping-math -funroll-loops -D_GLIBCXX_PARALLEL -march=native $(INCFLAGS) -fopenmp -Wall -Wno-strict-aliasing -Wextra -Wno-unused-variable -Wno-unused-parameter
LINKERFLAGS = -lz -lpqxx -lpq -lstxxl
DEBUGFLAGS = -g -ggdb $(INCFLAGS)
HEADERS=$(shell find . -name '*.hpp')

all:
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) src/main.cpp src/deps/MurmurHash3.cc -o bin/graphchi_handler $(LINKERFLAGS) 

	

	
