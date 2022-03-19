#include <cstdio>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <vector>
#include <random>
#define PORT 9527

void prepare_header(char *buf, const char *body) {
    sprintf(buf, "GET /?query=%s HTTP/1.1\r\n", body);
}

void prepare_mal_header(char *buf, const char *body) {
    sprintf(buf, "GET %s HTTP/1.1\r\n", body);
}

bool request(const std::string &request, const std::string &expected, bool malform) {
	int sock = 0;
	struct sockaddr_in serv_addr;
	char recv_buffer[1024] = {0};
    char send_buffer[1024] = {0};
	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		printf("\n Socket creation error \n");
		return -1;
	}

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(PORT);
	
	// Convert IPv4 and IPv6 addresses from text to binary form
	if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)
	{
		printf("\nInvalid address/ Address not supported \n");
		return -1;
	}

	if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	{
		printf("\nConnection Failed \n");
		return -1;
	}

    // std::cout << request << std::endl;
    if (malform) {
        prepare_mal_header(send_buffer, request.c_str());
    } else {
        prepare_header(send_buffer, request.c_str());
    }
	send(sock ,send_buffer, sizeof(send_buffer), 0);
	read(sock ,recv_buffer, sizeof(recv_buffer));
    
    // parse the result
    int cnt = 0;
    int ptr = 0;
    int len = strlen(recv_buffer);
    while (ptr < len) {
        if (recv_buffer[ptr] == '\n') {
            cnt++;
        }
        ptr++;
        if (cnt == 9) {
            break;
        }
    }
    if (ptr >= len) {
        return false;
    }
    std::string response = std::string(recv_buffer + ptr, recv_buffer + len);
    // std::cout << response << std::endl << expected << std::endl;
    close(sock);
    return response == expected;
}

char *request_table[][2] = {
    {
        // expression with double-quote test and ignore case test
        "c1%20$=%20\"ABC\"%20or%20c3%20&=%20\"\\\"\\\"\"", 
        "c1,c2,c3\nabc,qwe,123\n...,///,\"\"\"\nabc,cba,\"abc,cba\",\"\"abc,cba\"\"\n"
    },
    {
        // normal test
        "c1%20==%20\"qwe\"",
        "c1,c2,c3\nqwe,abc,123\nqwe,bcd,123\n"
    },
    {
        // normal test
        "c1%20==%20\"qwe\"%20or%20c2%20==%20\"zxc\"",
        "c1,c2,c3\nqwe,abc,123\nrty,zxc,123\nqwe,bcd,123\n"
    },
    {
        // expression with comma and empty test
        "c1%20==%20%22\\%22abc,cba\\%22%22",
        "c1,c2,c3\n"
    },
    {
        // expression with comma test
        "c2==%20%22\\%22abc,cba\\%22%22",
        "c1,c2,c3\nabc,cba,\"abc,cba\",\"\"abc,cba\"\"\n"
    },
    {
        // predicate and/or test
        "c1==%22$%$%22and%20c2%20==%20%22***&%22%20or%20c3%20==%20%22123%22",
        "c1,c2,c3\nqwe,abc,123\nabc,qwe,123\nrty,zxc,123\nqwe,bcd,123\n$%$,***&,$%\n"
    },
    {
        // contain test with special character
        "c3%20&=%20%22\\%22\\%22%22",
        "c1,c2,c3\n...,///,\"\"\"\nabc,cba,\"abc,cba\",\"\"abc,cba\"\"\n"
    },
    {
        // empty column name test
        "%20==%20%22qwe%22%20or%20c1%20==%20%22asd%22",
        "c1,c2,c3\nqwe,abc,123\nabc,qwe,123\nqwe,bcd,123\nasd,zcc,456\n",
    },
    {
        // not equal test and and/or test
        "c1%20!=%20\"qwe\"%20and%20c2%20==%20\"qwe\"%20or%20c1%20==%20\"qwe\"%20and%20c2%20!=%20\"qwe\"",
        "c1,c2,c3\nqwe,abc,123\nabc,qwe,123\nqwe,bcd,123\n"
    },
    {
        // # character test, we need to replace it with %23
        "%20c1%20&=%20%22%23%22%20and%20c3%20&=%20%22%3C%3E%22",
        "c1,c2,c3\n#abc,+++---,!@#$%<>\n",
    },
    {
        // invalid column name test
        "%20abc%20==%20\"a\"",
        "invalid column name",
    },
    {
        // malformed operator test
        "%20c1%20*=%20\"abc\"",
        "failed to parse operator"
    },
    {
        // wrong format test
        "%20c1%20==%20\"abc and%20c2",
        "wrong predicate format, expected \"",
    },
    {
        // wrong format test
        "/?request=%20c1%20==%20%22qwe%22",
        "wrong format, expected /?query=<query string>"
    },
    {
        NULL,
        NULL,
    }
};

const int iterations = 100;
const int workers = 6;

void worker(int total) {
    std::random_device device;
    std::mt19937 rnd(device());
    std::uniform_int_distribution<int> dis(0, total - 1);
    for (int i = 0; i < iterations; i++) {
        int ptr = dis(rnd);
        bool res;
        if (request_table[ptr][0][0] == '/') {
            res = request(request_table[ptr][0], request_table[ptr][1], true);
        } else {
            res = request(request_table[ptr][0], request_table[ptr][1], false);
        }
        if (res == false) {
            std::cout << "FATAL: failed test" << std::endl;
        }
    }
}

int main() {
    int cnt = 0;
    while (request_table[cnt][0] != NULL) {
        cnt++;
    }

    std::vector<std::thread> list;
    // spawn 6 client to request
    for (int i = 0; i < workers; i++) {
        list.emplace_back(std::thread{worker, cnt});
    }
    for (int i = 0; i < workers; i++) {
        list[i].join();
    }
    std::cout << "test: OK" << std::endl;
	return 0;
}
