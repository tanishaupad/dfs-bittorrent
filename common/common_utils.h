#ifndef COMMON_UTILS_H
#define COMMON_UTILS_H

#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <utility>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>

namespace Utils {

inline std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

inline std::string trim(const std::string &str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (std::string::npos == first) {
        return "";
    }
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, (last - first + 1));
}

// Parses IP and port from "IP:Port" or "IP Port"
inline std::pair<std::string, int> parse_ip_port(const std::string &s) {
    size_t colon = s.find(':');
    if (colon != std::string::npos) {
        return {s.substr(0, colon), std::stoi(s.substr(colon + 1))};
    }
    size_t space = s.find(' ');
    if (space != std::string::npos) {
        return {s.substr(0, space), std::stoi(s.substr(space + 1))};
    }
    return {s, 0};
}

// Parses tracker_info.txt, returns vector of pairs of {IP, Port}
inline std::vector<std::pair<std::string, int>> parse_tracker_info(const std::string &filepath) {
    std::vector<std::pair<std::string, int>> trackers;
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open tracker info file: " << filepath << std::endl;
        return trackers;
    }
    std::string line;
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.empty()) continue;
        // The file can contain IP and Port separated by space or colon
        auto p = parse_ip_port(line);
        if (p.second != 0) {
            trackers.push_back(p);
        }
    }
    return trackers;
}

// Robust read to ensure 'count' bytes are read from socket
inline ssize_t read_all(int socket_fd, void *buf, size_t count) {
    size_t total_read = 0;
    char *ptr = static_cast<char*>(buf);
    while (total_read < count) {
        ssize_t bytes_read = recv(socket_fd, ptr + total_read, count - total_read, 0);
        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            return -1; // Error
        } else if (bytes_read == 0) {
            return total_read; // Connection closed early
        }
        total_read += bytes_read;
    }
    return total_read;
}

// Robust write to ensure 'count' bytes are written to socket
inline ssize_t write_all(int socket_fd, const void *buf, size_t count) {
    size_t total_written = 0;
    const char *ptr = static_cast<const char*>(buf);
    while (total_written < count) {
        ssize_t bytes_written = send(socket_fd, ptr + total_written, count - total_written, 0);
        if (bytes_written < 0) {
            if (errno == EINTR) continue;
            return -1; // Error
        }
        total_written += bytes_written;
    }
    return total_written;
}

// Sends a string message followed by a newline over TCP
inline bool send_msg(int socket_fd, const std::string &msg) {
    std::string terminated_msg = msg + "\n";
    ssize_t ret = write_all(socket_fd, terminated_msg.data(), terminated_msg.size());
    return ret == static_cast<ssize_t>(terminated_msg.size());
}

// Receives a string message terminated by a newline from TCP
inline std::string recv_msg(int socket_fd) {
    std::string msg;
    char ch;
    while (true) {
        ssize_t ret = recv(socket_fd, &ch, 1, 0);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return ""; // Error
        } else if (ret == 0) {
            break; // Connection closed
        }
        if (ch == '\n') {
            break; // Reached end of line
        }
        msg += ch;
    }
    return msg;
}

// Extracts filename from path
inline std::string get_filename(const std::string &path) {
    size_t last_slash = path.find_last_of("/\\");
    if (last_slash == std::string::npos) {
        return path;
    }
    return path.substr(last_slash + 1);
}

} // namespace Utils

#endif // COMMON_UTILS_H
