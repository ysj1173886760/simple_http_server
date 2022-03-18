CXXFLAGS = -pthread -Wall -Wextra

server: server.cpp
	$(CXX) $(CXXFLAGS) server.cpp -o server