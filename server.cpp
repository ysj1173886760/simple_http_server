//
//  sample.cc
//
//  Copyright (c) 2019 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <vector>
#include <iostream>
#include <thread>

#include <unistd.h>
#include <cstdio>
#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <cstring>
#include "utils.cpp"

std::vector<std::vector<std::string>> _database;

void print() {
    for (unsigned int i = 0; i < _database.size(); i++) {
        std::cout << _database[i][0] << " " << _database[i][1] << " " << _database[i][2] << std::endl;
    }
}

enum class CSVState {
    UnquotedField,
    QuotedField,
    QuotedQuote
};

std::vector<std::string> readCSVRow(const std::string &row) {
    CSVState state = CSVState::UnquotedField;
    std::vector<std::string> fields;
    std::string cur;
    for (char c : row) {
        switch (state) {
            case CSVState::UnquotedField:
                switch (c) {
                    case ',': // end of field
                              // trim the space
                              cur = trim_copy(cur);
                              fields.emplace_back(cur);
                              cur = "";
                              break;
                    case '"': state = CSVState::QuotedField;
                              break;
                    default:  cur += c;
                              break; }
                break;
            case CSVState::QuotedField:
                switch (c) {
                    case '"': state = CSVState::QuotedQuote;
                              break;
                    default:  cur += c;
                              break; }
                break;
            case CSVState::QuotedQuote:
                switch (c) {
                    case ',': // , after closing quote
                              cur = trim_copy(cur);
                              fields.emplace_back(cur);
                              cur = "";
                              state = CSVState::UnquotedField;
                              break;
                    case '"': // "" -> "
                              cur += '"';
                              state = CSVState::QuotedField;
                              break;
                    default:  // end of quote
                              state = CSVState::UnquotedField;
                              break; }
                break;
        }
    }
    if (!cur.empty()) {
        cur = trim_copy(cur);
        fields.emplace_back(cur);
    }
    return fields;
}

/// Read CSV file, Excel dialect. Accept "quoted fields ""with quotes"""
std::vector<std::vector<std::string>> readCSV(std::istream &in) {
    std::vector<std::vector<std::string>> table;
    std::string row;
    while (!in.eof()) {
        std::getline(in, row);
        if (in.bad() || in.fail()) {
            break;
        }
        auto fields = readCSVRow(row);
        table.emplace_back(fields);
    }
    return table;
}

void prepare_response(char *buffer, const char *body) {
    auto fmt =
            "HTTP/1.1 200 OK \r\n"
            "Server: simplehttp\r\n"
            "Accept-Encoding:gzip, deflate\r\n"
            "Accept-Language:zh-CN,zh;q=0.9\r\n"
            "Cache-Control:no-cache\r\n"
            "Connection:keep-alive\r\n"
            "Content-Type:text/plain\r\n"
            "Content-Length: %d\r\n"
            "\r\n"
            "%s";
    sprintf(buffer, fmt, strlen(body), body);
}

std::string process_request(const std::string &param) {
    std::cout << "params: " << std::endl;
    std::cout << param << std::endl;
    return _database[0][0];
}

void send_error(int fd, char *error) {
    char response[1024] = {0};
    prepare_response(response, error);
    send(fd, response, 1024, 0);
}

enum Operator {
    EQUAL = 0,
    NOT_EQUAL,
    CASE_INSENSITIVE,
    CONTAINS,
};

struct Predicate {
    std::string column;
    std::string target;
    int op;
    Predicate(std::string _column, std::string _target, int _operator):
        column(_column), target(_target), op(_operator) {}
};

bool valid_column_character(char ch) {
    if (ch >= 'A' && ch <= 'Z') {
        return true;
    }
    if (ch >= 'a' && ch <= 'z') {
        return true;
    }
    if (ch >= '0' && ch <= '9') {
        return true;
    }
    return false;
}

int get_operator(const std::string &param, int cur) {
    if (cur + 1 >= param.size()) {
        return -1;
    }
    if (param[cur + 1] != '=') {
        return -1;
    }
    if (param[cur] == '=') {
        return EQUAL;
    } else if (param[cur] == '!') {
        return NOT_EQUAL;
    } else if (param[cur] == '$') {
        return CASE_INSENSITIVE;
    } else if (param[cur] == '&') {
        return CONTAINS;
    }

    return -1;
}

// return NULL if parse succeed
// otherwise return the error message
// operator priority is (xxx and xxx) or (xxx and xxx) or (xxx and xxx)
char *parse_predicate(std::vector<std::vector<Predicate>> &predicates, const std::string &param) {
    int cur = 0;
    std::vector<Predicate> and_field;
    while (cur < param.size()) {
        // skip space
        while (cur < param.size() && param[cur] == ' ') {
            cur++;
        }
        if (cur >= param.size()) {
            break;
        }
        // should be column name
        std::string column;
        while (cur < param.size() && valid_column_character(param[cur])) {
            column += param[cur];
            cur++;
        }
        if (cur >= param.size()) {
            return "failed to parse predicate";
        }
        
        // then operator
        // skip space
        while (cur < param.size() && param[cur] == ' ') {
            cur++;
        }
        if (cur >= param.size()) {
            return "failed to find operator";
        }

        int op= get_operator(param, cur);
        if (op == -1) {
            return "failed to parse operator";
        }
        cur += 2;

        // skip space and parse target
        while (cur < param.size() && param[cur] == ' ') {
            cur++;
        }
        if (cur >= param.size()) {
            return "failed to find target";
        }

        std::string target;
        int cnt = 0;
        while (cur < param.size()) {
            if (param[cur] == '\\') {
                if (cur + 1 >= param.size()) {
                    return "parse failed";
                }
                target += param[cur + 1];
                cur += 2;
            } else {
                if (param[cur] == '"') {
                    cnt++;
                    if (cnt == 2) {
                        break;
                    }
                } else {
                    target += param[cur];
                }
                cur++;
            }
        }
        std::cout << "target is : " << target << std::endl;

        if (cnt != 2) {
            return "wrong predicate format, expected \"";
        }
        cur++;

        // parsing is done
        and_field.push_back(Predicate(column, target, op));

        // try to find next "or"
        while (cur < param.size() && param[cur] == ' ') {
            cur++;
        }
        if (cur >= param.size()) {
            break;
        }
        if (cur + 1 < param.size() && param[cur] == 'o' && param[cur + 1] == 'r') {
            predicates.push_back(and_field);
            and_field.clear();
            cur += 2;
        } else if (cur + 2 < param.size() && param[cur] == 'a' && param[cur + 1] == 'n' && param[cur + 2] == 'd') {
            cur += 3;
        }
    }
    if (!and_field.empty()) {
        predicates.push_back(and_field);
    }
    return NULL;
}

void print_predicate(const std::vector<std::vector<Predicate>> &predicates) {
    for (int i = 0; i < predicates.size(); i++) {
        for (int j = 0; j < predicates[i].size(); j++) {
            std::cout << predicates[i][j].column << " " << predicates[i][j].target << " " << predicates[i][j].op << std::endl;
        }
        std::cout << std::endl;
    }
}

bool check_valid_column(const std::string &column) {
    for (int i = 0; i < _database[0].size(); i++) {
        if (column == _database[0][i]) {
            return true;
        }
    }
    return false;
}

bool cmp_ignore_case(const std::string &s1, const std::string &s2) {
    if (s1.size() != s2.size()) {
        return false;
    }
    for (int i = 0; i < s1.size(); i++) {
        char l1 = (s1[i] >= 'A' && s1[i] <= 'Z') ? s1[i] - 'A' + 'a' : s1[i];
        char l2 = (s2[i] >= 'A' && s2[i] <= 'Z') ? s2[i] - 'A' + 'a' : s2[i];
        if (l1 != l2) {
            return false;
        }
    }
    return true;
}

//  TODO: use Trie or kmp to optimize
bool contains(const std::string &s1, const std::string &s2) {
    int l2 = s2.size();
    for (int i = 0; i < s1.size() - l2 + 1; i++) {
        if (s1.substr(i, l2) == s2) {
            return true;
        }
    }
    return false;
}

bool check_operation(Predicate &predicate, const std::string &column) {
    switch (predicate.op) {
    case EQUAL:
        // std::cout << column << "==" << predicate.target << std::endl;
        return column == predicate.target;
        break;
    case NOT_EQUAL:
        return column != predicate.target;
        break;
    case CASE_INSENSITIVE:
        return cmp_ignore_case(column, predicate.target);
        break;
    case CONTAINS:
        return contains(column, predicate.target);
        break;
    }
}

int get_column(const std::string &column) {
    for (int i = 0; i < 3; i++) {
        if (_database[0][i] == column) {
            return i;
        }
    }
    // fatal error
    std::cout << "fatal error: failed to find column" << std::endl;
    exit(-1);
}

bool check_predicates(std::vector<Predicate> &predicates, std::vector<std::string> &row) {
    bool condition = true;
    for (int i = 0; i < predicates.size(); i++) {
        if (!condition) {
            return false;
        }
        if (predicates[i].column.empty()) {
            condition = condition && (check_operation(predicates[i], row[0]) ||
                                      check_operation(predicates[i], row[1]) ||
                                      check_operation(predicates[i], row[2]));
        } else {
            condition = condition && check_operation(predicates[i], row[get_column(predicates[i].column)]);
        }
    }
    return condition;
}

void worker(int fd) {
    char buffer[1024] = {0};
    int size = read(fd, buffer, 1024);
    int cnt1 = 0, cnt2 = 0;
    while (buffer[cnt1] != ' ') {
        cnt1++;
    }
    cnt2 = cnt1 + 1;
    while (buffer[cnt2] != ' ') {
        cnt2++;
    }
    std::string s = decode_url(std::string(buffer + cnt1, buffer + cnt2), false);
    // std::cout << s << std::endl;

    int cnt = 0;
    // find first '='
    while (cnt < s.size() && s[cnt] != '=') {
        cnt++;
    }
    cnt++;
    if (cnt >= s.size()) {
        // can't find params, return with error
        send_error(fd, "failed to get params");
        return;
    }

    std::string raw_param = s.substr(cnt, s.size() - cnt);
    // std::cout << raw_param << std::endl;

    std::vector<std::vector<Predicate>> predicates;
    char *err = parse_predicate(predicates, raw_param);

    if (err != 0) {
        send_error(fd, err);
        return;
    }
    print_predicate(predicates);

    // check the predicates to ensure there column name are all vaild
    for (int i = 0; i < predicates.size(); i++) {
        for (int j = 0; j < predicates[i].size(); j++) {
            if (predicates[i][j].column.size() > 0 && !check_valid_column(predicates[i][j].column)) {
                std::cout << predicates[i][j].column << std::endl;
                send_error(fd, "invalid column name");
                return;
            }
        }
    }

    // processing
    std::vector<std::vector<std::string>> res;
    res.push_back(_database[0]);
    for (int i = 1; i < _database.size(); i++) {
        // for every row, try to apply the predicate
        bool condition = false;
        for (int j = 0; j < predicates.size(); j++) {
            condition = condition || check_predicates(predicates[j], _database[i]);
            if (condition) {
                break;
            }
        }
        if (condition) {
            res.push_back(_database[i]);
        }
    }

    std::string body;
    for (int i = 0; i < res.size(); i++) {
        body += res[i][0] + "," + res[i][1] + "," + res[i][2] + "\n";
    }
    // because we have about 200 bytes header
    char *buf = (char*)malloc(body.size() + 200);
    memset(buf, 0, body.size() + 200);
    prepare_response(buf, body.c_str());
    // std::cout << strlen(buf) << " " << body.size() << std::endl;
    send(fd, buf, strlen(buf), 0);
    free(buf);
}


int main(void) {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    int port = 9527;

    std::string filename = "data.csv";

    std::ifstream reader(filename, std::ios::in);
    // read data into memory
    _database = readCSV(reader);

    // test reading
    // std::cout << "result ..............." << std::endl;
    print();

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        std::cout << "socket failed";
        exit(EXIT_FAILURE);
    }
       
    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                                                  &opt, sizeof(opt)))
    {
        std::cout << "setsockopt";
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
       
    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cout << "bind failed";
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0) {
        std::cout << "listen";
        exit(EXIT_FAILURE);
    }
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            continue;
        }
        std::thread t(worker, new_socket);
        t.detach();
    }

    return 0;
}

// use %23 to replace #

// use this in browser
// http://localhost:9527/?query=C1%20==%20%22A%22%20and%20C2%20&=%20%22B/*\%22?%a+bc-=%23&aqaq%22

// use this in curl
// http://localhost:8080/?query=C1%20==%20%22A%22%20or%20C2%20&=%20%22B/*\%22?%a+bc-=%23&aqaq%22
