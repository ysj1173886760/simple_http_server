CXXFLAGS = -pthread

server: server.cpp utils.cpp
	$(CXX) $(CXXFLAGS) server.cpp utils.cpp -o server
client: client.cpp
	$(CXX) $(CXXFLAGS) client.cpp -o client