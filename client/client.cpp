#include "client.h"
#include "../common/common_utils.h"
#include "../common/sha1.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cstring>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <csignal>
#include <sys/stat.h>

Client client_inst;

// PieceQueue struct definition for Rarest-first scheduling
struct PieceQueue {
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<size_t> pending_pieces; // piece indices sorted by rarest first
    std::vector<bool> in_progress;
    std::vector<std::vector<bool>> rejected_by_peer;
    std::vector<size_t> frequencies;
    size_t active_pieces = 0;

    void initialize(size_t num_pieces, const std::vector<std::string> &peer_bitfields) {
        frequencies.assign(num_pieces, 0);
        for (size_t i = 0; i < num_pieces; i++) {
            for (const auto &bf : peer_bitfields) {
                if (i < bf.size() && bf[i] == '1') {
                    frequencies[i]++;
                }
            }
        }

        for (size_t i = 0; i < num_pieces; i++) pending_pieces.push_back(i);

        // Sort rarest-first
        std::sort(pending_pieces.begin(), pending_pieces.end(), [this](size_t a, size_t b) {
            return frequencies[a] < frequencies[b];
        });

        in_progress.assign(num_pieces, false);
        rejected_by_peer.assign(peer_bitfields.size(), std::vector<bool>(num_pieces, false));
    }

    bool pop_piece(size_t peer_index, const std::string &peer_bitfield, size_t &piece_idx) {
        std::unique_lock<std::mutex> lock(mtx);
        while (true) {
            for (auto it = pending_pieces.begin(); it != pending_pieces.end(); ++it) {
                size_t p = *it;
                if (p < peer_bitfield.size() && peer_bitfield[p] == '1' &&
                    !rejected_by_peer[peer_index][p]) {
                    piece_idx = p;
                    in_progress[p] = true;
                    ++active_pieces;
                    pending_pieces.erase(it);
                    return true;
                }
            }
            if (active_pieces == 0) return false;
            cv.wait(lock);
        }
    }

    void retry(size_t peer_index, size_t piece_idx, bool reject_peer) {
        std::lock_guard<std::mutex> lock(mtx);
        if (reject_peer) rejected_by_peer[peer_index][piece_idx] = true;
        in_progress[piece_idx] = false;
        --active_pieces;
        auto it = std::lower_bound(pending_pieces.begin(), pending_pieces.end(), piece_idx, [this](size_t a, size_t b) {
            return frequencies[a] < frequencies[b];
        });
        pending_pieces.insert(it, piece_idx);
        cv.notify_all();
    }

    void complete(size_t piece_idx) {
        std::lock_guard<std::mutex> lock(mtx);
        in_progress[piece_idx] = false;
        --active_pieces;
        cv.notify_all();
    }
};

void Client::load_local_files_db() {
    std::lock_guard<std::mutex> lock(files_mtx);
    local_files.clear();
    std::ifstream file(local_files_db);
    if (!file.is_open()) return;
    std::string line;
    while (std::getline(file, line)) {
        line = Utils::trim(line);
        if (line.empty()) continue;
        std::vector<std::string> tokens = Utils::split(line, ' ');
        if (tokens.size() >= 3) {
            std::string key = tokens[0] + ":" + tokens[1];
            // Reconstruct path if it had spaces
            std::string path = tokens[2];
            for (size_t i = 3; i < tokens.size(); i++) {
                path += " " + tokens[i];
            }
            local_files[key] = path;
        }
    }
}

void Client::save_local_files_db() {
    std::lock_guard<std::mutex> lock(files_mtx);
    std::ofstream file(local_files_db, std::ios::trunc);
    if (!file.is_open()) return;
    for (const auto &pair : local_files) {
        size_t colon = pair.first.find(':');
        std::string g_id = pair.first.substr(0, colon);
        std::string f_name = pair.first.substr(colon + 1);
        file << g_id << " " << f_name << " " << pair.second << "\n";
    }
}

void Client::add_shared_file(const std::string &group_id, const std::string &file_name, const std::string &filepath) {
    {
        std::lock_guard<std::mutex> lock(files_mtx);
        std::string key = group_id + ":" + file_name;
        local_files[key] = filepath;
    }
    save_local_files_db();
}

void Client::announce_verified_pieces(const std::string &group_id, const std::string &file_name,
                                     const std::string &filepath, const std::string &sharing_user) {
    const std::string key = group_id + ":" + file_name;
    {
        std::lock_guard<std::mutex> lock(downloads_mtx);
        auto it = downloads.find(key);
        if (it == downloads.end() || it->second.downloaded_pieces == 0 ||
            it->second.peer_announced || it->second.announcing) return;
        it->second.announcing = true;
    }

    // The peer server must have a path before the tracker advertises this peer.
    {
        std::lock_guard<std::mutex> lock(files_mtx);
        local_files[key] = filepath;
    }
    const std::string response = send_to_tracker("REGISTER_SEEDER " + group_id + " " +
                                                 sharing_user + " " + file_name);
    {
        std::lock_guard<std::mutex> lock(downloads_mtx);
        auto it = downloads.find(key);
        if (it != downloads.end()) {
            it->second.peer_announced = response.rfind("SUCCESS", 0) == 0;
            it->second.announcing = false;
        }
    }
}

bool Client::get_shared_file(const std::string &group_id, const std::string &file_name, std::string &filepath) {
    std::lock_guard<std::mutex> lock(files_mtx);
    std::string key = group_id + ":" + file_name;
    auto it = local_files.find(key);
    if (it != local_files.end()) {
        filepath = it->second;
        return true;
    }
    return false;
}

void Client::update_download_progress(const std::string &group_id, const std::string &file_name, size_t downloaded, size_t total, const std::vector<bool> &status, bool completed, const std::string &status_str) {
    std::lock_guard<std::mutex> lock(downloads_mtx);
    std::string key = group_id + ":" + file_name;
    DownloadProgress &dp = downloads[key];
    dp.group_id = group_id;
    dp.file_name = file_name;
    dp.downloaded_pieces = downloaded;
    dp.total_pieces = total;
    dp.pieces_status = status;
    dp.is_completed = completed;
    dp.status_str = status_str;
}

void Client::show_downloads() {
    std::lock_guard<std::mutex> lock(downloads_mtx);
    if (downloads.empty()) {
        std::cout << "No active or past downloads." << std::endl;
        return;
    }
    for (const auto &pair : downloads) {
        const auto &dp = pair.second;
        if (dp.is_completed) {
            std::cout << "[C] [" << dp.group_id << "] " << dp.file_name << std::endl;
        } else {
            int pct = 0;
            if (dp.total_pieces > 0) {
                pct = (dp.downloaded_pieces * 100) / dp.total_pieces;
            }
            std::cout << "[D] [" << dp.group_id << "] " << dp.file_name << " - " << pct << "%" << std::endl;
        }
    }
}

bool Client::connect_to_tracker(int &sock) {
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    struct timeval tv;
    tv.tv_sec = 3;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));

    std::string ip = trackers[active_tracker_idx].first;
    int port = trackers[active_tracker_idx].second;

    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
        close(sock);
        return false;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return false;
    }
    return true;
}

std::string Client::send_to_tracker(const std::string &cmd) {
    int sock = -1;
    bool connected = connect_to_tracker(sock);

    if (!connected) {
        active_tracker_idx = 1 - active_tracker_idx;
        std::cout << "[System] Primary tracker offline. Attempting failover to tracker at "
                  << trackers[active_tracker_idx].first << ":" << trackers[active_tracker_idx].second << "..." << std::endl;

        connected = connect_to_tracker(sock);
        if (!connected) {
            return "ERROR both trackers are offline";
        }

        if (logged_in && !username.empty() && !password.empty()) {
            std::string pwd_hash = SHA1::hash_string(password);
            std::string login_cmd = "LOGIN " + username + " " + pwd_hash + " " + my_ip + " " + std::to_string(my_port);
            if (Utils::send_msg(sock, login_cmd)) {
                std::string res = Utils::recv_msg(sock);
                if (res.rfind("SUCCESS", 0) != 0) {
                    close(sock);
                    return "ERROR session recovery failed on backup tracker: " + res;
                }
            } else {
                close(sock);
                return "ERROR connection to backup tracker lost during recovery";
            }
        }
    }

    if (!Utils::send_msg(sock, cmd)) {
        close(sock);
        return "ERROR sending command to tracker failed";
    }

    std::string response = Utils::recv_msg(sock);
    close(sock);
    return response;
}

// C2C Connection Handler
void peer_connection_handler(int sock, Client *cl) {
    while (true) {
        std::string msg = Utils::recv_msg(sock);
        if (msg.empty()) break;

        std::vector<std::string> tokens = Utils::split(msg, ' ');
        if (tokens.empty()) continue;

        if (tokens[0] == "GET_BITFIELD") {
            if (tokens.size() < 3) {
                Utils::send_msg(sock, "ERROR invalid args");
                continue;
            }
            std::string g_id = tokens[1];
            std::string f_name = tokens[2];
            std::string filepath;
            if (cl->get_shared_file(g_id, f_name, filepath)) {
                std::ifstream file(filepath, std::ios::binary | std::ios::ate);
                if (!file.is_open()) {
                    Utils::send_msg(sock, "ERROR file not found");
                    continue;
                }
                size_t size = file.tellg();
                size_t pieces = (size + 524288 - 1) / 524288;

                std::string key = g_id + ":" + f_name;
                std::lock_guard<std::mutex> d_lock(cl->downloads_mtx);
                auto it = cl->downloads.find(key);
                std::string bitfield(pieces, '1'); // default is completed
                if (it != cl->downloads.end()) {
                    for (size_t i = 0; i < pieces; i++) {
                        bitfield[i] = (it->second.pieces_status[i] ? '1' : '0');
                      }
                }
                Utils::send_msg(sock, "BITFIELD " + bitfield);
            } else {
                Utils::send_msg(sock, "ERROR not sharing this file");
            }
        }
        else if (tokens[0] == "GET_PIECE") {
            if (tokens.size() < 4) {
                Utils::send_msg(sock, "ERROR invalid args");
                continue;
            }
            std::string g_id = tokens[1];
            std::string f_name = tokens[2];
            size_t piece_idx = std::stoull(tokens[3]);
            std::string filepath;
            if (cl->get_shared_file(g_id, f_name, filepath)) {
                std::string key = g_id + ":" + f_name;
                bool have_piece = true;
                {
                    std::lock_guard<std::mutex> d_lock(cl->downloads_mtx);
                    auto it = cl->downloads.find(key);
                    if (it != cl->downloads.end()) {
                        if (piece_idx >= it->second.pieces_status.size() || !it->second.pieces_status[piece_idx]) {
                            have_piece = false;
                        }
                    }
                }

                if (!have_piece) {
                    Utils::send_msg(sock, "ERROR piece not downloaded yet");
                    continue;
                }

                std::ifstream file(filepath, std::ios::binary);
                if (!file.is_open()) {
                    Utils::send_msg(sock, "ERROR file error");
                    continue;
                }

                file.seekg(piece_idx * 524288, std::ios::beg);
                std::vector<char> buf(524288);
                file.read(buf.data(), 524288);
                std::streamsize read_bytes = file.gcount();

                if (read_bytes < 0) {
                    Utils::send_msg(sock, "ERROR read error");
                    continue;
                }

                std::string header = "OK " + std::to_string(read_bytes) + "\n";
                Utils::write_all(sock, header.data(), header.size());
                Utils::write_all(sock, buf.data(), read_bytes);
            } else {
                Utils::send_msg(sock, "ERROR not sharing this file");
            }
        }
    }
    close(sock);
}

// C2C Listener Thread
void peer_server(int server_fd, Client *cl) {
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd >= 0) {
            std::thread t(peer_connection_handler, client_fd, cl);
            t.detach();
        }
    }
}

// Individual Peer Downloader Worker Thread
void peer_download_worker(size_t peer_index, int sock, std::string peer_bitfield, std::string group_id, std::string file_name, std::string dest_path, std::string sharing_user, size_t file_size, const std::vector<std::string> &expected_hashes, PieceQueue *queue, Client *cl) {
    size_t piece_idx;
    static std::mutex file_write_mtx;

    while (queue->pop_piece(peer_index, peer_bitfield, piece_idx)) {
        std::string req = "GET_PIECE " + group_id + " " + file_name + " " + std::to_string(piece_idx);
        if (!Utils::send_msg(sock, req)) {
            queue->retry(peer_index, piece_idx, false);
            break;
        }

        std::string response = Utils::recv_msg(sock);
        if (response.empty()) {
            queue->retry(peer_index, piece_idx, false);
            break;
        }

        std::vector<std::string> tokens = Utils::split(response, ' ');
        if (tokens.size() < 2 || tokens[0] != "OK") {
            queue->retry(peer_index, piece_idx, true);
            continue;
        }

        size_t data_len = 0;
        try { data_len = std::stoull(tokens[1]); }
        catch (const std::exception &) {
            queue->retry(peer_index, piece_idx, false);
            break;
        }
        const size_t offset = piece_idx * 524288;
        const size_t expected_len = std::min<size_t>(524288, file_size - offset);
        if (data_len != expected_len) {
            queue->retry(peer_index, piece_idx, false);
            break;
        }
        std::vector<char> buffer(data_len);
        ssize_t bytes_read = Utils::read_all(sock, buffer.data(), data_len);
        if (bytes_read != static_cast<ssize_t>(data_len)) {
            queue->retry(peer_index, piece_idx, false);
            break;
        }

        // Verify hash
        SHA1 piece_sha;
        piece_sha.update((const unsigned char*)buffer.data(), data_len);
        std::string calculated_hash = piece_sha.final();

        if (calculated_hash != expected_hashes[piece_idx]) {
            queue->retry(peer_index, piece_idx, true);
            continue; // Hash failed, retry
        }

        // Write piece to disk
        {
            std::lock_guard<std::mutex> file_lock(file_write_mtx);
            std::fstream file(dest_path, std::ios::in | std::ios::out | std::ios::binary);
            if (file.is_open()) {
                file.seekp(piece_idx * 524288, std::ios::beg);
                file.write(buffer.data(), data_len);
                file.flush();
                if (!file.good()) {
                    queue->retry(peer_index, piece_idx, false);
                    break;
                }
                file.close();
            } else {
                queue->retry(peer_index, piece_idx, false);
                break;
            }
        }

        // Update progress
        {
            std::lock_guard<std::mutex> lock(cl->downloads_mtx);
            std::string key = group_id + ":" + file_name;
            auto it = cl->downloads.find(key);
            if (it != cl->downloads.end()) {
                it->second.pieces_status[piece_idx] = true;
                it->second.downloaded_pieces++;
            }
        }
        queue->complete(piece_idx);
        cl->announce_verified_pieces(group_id, file_name, dest_path, sharing_user);
    }
    close(sock);
}

// Download Coordinator Thread
void download_coordinator_thread(std::string group_id, std::string file_name, std::string dest_path, std::string sharing_user, size_t file_size, std::string file_sha1, std::vector<std::string> expected_hashes, std::vector<std::pair<std::string, int>> peer_addrs, Client *cl) {
    std::vector<int> peer_socks;
    std::vector<std::string> peer_bitfields;

    // Connect to all peers and get their bitfields
    for (const auto &peer : peer_addrs) {
        // Do not connect to ourselves
        if (peer.first == cl->my_ip && peer.second == cl->my_port) {
            continue;
        }

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) continue;

        struct timeval tv;
        tv.tv_sec = 3;
        tv.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));

        sockaddr_in peer_addr;
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_port = htons(peer.second);
        if (inet_pton(AF_INET, peer.first.c_str(), &peer_addr.sin_addr) <= 0) {
            close(sock);
            continue;
        }

        if (connect(sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
            close(sock);
            continue;
        }

        // Request bitfield
        std::string req = "GET_BITFIELD " + group_id + " " + file_name;
        if (!Utils::send_msg(sock, req)) {
            close(sock);
            continue;
        }

        std::string res = Utils::recv_msg(sock);
        std::vector<std::string> tokens = Utils::split(res, ' ');
        if (tokens.size() >= 2 && tokens[0] == "BITFIELD") {
            peer_socks.push_back(sock);
            peer_bitfields.push_back(tokens[1]);
        } else {
            close(sock);
        }
    }

    std::string key = group_id + ":" + file_name;

    if (peer_socks.empty()) {
        std::cout << "\nError: No seeders online for file " << file_name << std::endl;
        {
            std::lock_guard<std::mutex> lock(cl->downloads_mtx);
            cl->downloads[key].status_str = "Failed (No active seeders)";
        }
        return;
    }

    PieceQueue queue;
    queue.initialize(expected_hashes.size(), peer_bitfields);

    std::vector<std::thread> workers;
    for (size_t i = 0; i < peer_socks.size(); i++) {
        workers.push_back(std::thread(peer_download_worker, i, peer_socks[i], peer_bitfields[i], group_id, file_name, dest_path, sharing_user, file_size, expected_hashes, &queue, cl));
    }

    for (auto &t : workers) {
        t.join();
    }

    // Check final status
    bool success = false;
    {
        std::lock_guard<std::mutex> lock(cl->downloads_mtx);
        auto it = cl->downloads.find(key);
        if (it != cl->downloads.end()) {
            if (it->second.downloaded_pieces == it->second.total_pieces) {
                success = true;
                it->second.status_str = "Verifying";
            } else {
                it->second.status_str = "Failed";
            }
        }
    }

    if (success) {
        // Verify entire file hash
        std::string final_file_sha = SHA1::hash_file(dest_path);
        if (final_file_sha == file_sha1) {
            cl->announce_verified_pieces(group_id, file_name, dest_path, sharing_user);
            {
                std::lock_guard<std::mutex> lock(cl->downloads_mtx);
                cl->downloads[key].is_completed = true;
                cl->downloads[key].status_str = "Completed";
            }
            std::cout << "\n[C] [" << group_id << "] " << file_name << std::endl;
            cl->add_shared_file(group_id, file_name, dest_path);
        } else {
            std::cout << "\nError: Integrity check failed for " << file_name << ". Redownload required." << std::endl;
            std::lock_guard<std::mutex> lock(cl->downloads_mtx);
            cl->downloads[key].is_completed = false;
            cl->downloads[key].status_str = "Failed (Integrity error)";
        }
    } else {
        std::cout << "\nError: Download failed or incomplete for " << file_name << std::endl;
    }
}

// User CLI input loop
void run_cli(Client *cl) {
    std::string line;
    while (true) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }

        line = Utils::trim(line);
        if (line.empty()) continue;

        std::vector<std::string> tokens = Utils::split(line, ' ');

        if (tokens.size() >= 4 && tokens[0] == "create" && tokens[1] == "user") {
            std::string uid = tokens[2];
            std::string pwd = tokens[3];
            std::string pwd_hash = SHA1::hash_string(pwd);

            std::string cmd = "CREATE_USER " + uid + " " + pwd_hash;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 3 && tokens[0] == "login") {
            if (cl->logged_in) {
                std::cout << "ERROR already logged in. Please logout first." << std::endl;
                continue;
            }
            std::string uid = tokens[1];
            std::string pwd = tokens[2];
            std::string pwd_hash = SHA1::hash_string(pwd);

            std::string cmd = "LOGIN " + uid + " " + pwd_hash + " " + cl->my_ip + " " + std::to_string(cl->my_port);
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
            if (res.rfind("SUCCESS", 0) == 0) {
                cl->logged_in = true;
                cl->username = uid;
                cl->password = pwd;
            }
        }
        else if (tokens.size() >= 3 && tokens[0] == "create" && tokens[1] == "group") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string cmd = "CREATE_GROUP " + gid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 3 && tokens[0] == "join" && tokens[1] == "group") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string cmd = "JOIN_GROUP " + gid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 3 && tokens[0] == "leave" && tokens[1] == "group") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string cmd = "LEAVE_GROUP " + gid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 2 && tokens[0] == "list" && tokens[1] == "groups") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string cmd = "LIST_GROUPS";
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 3 && tokens[0] == "list" && tokens[1] == "requests") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string cmd = "LIST_REQUESTS " + gid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 4 && tokens[0] == "accept" && tokens[1] == "request") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string uid = tokens[3];
            std::string cmd = "ACCEPT_REQUEST " + gid + " " + uid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 1 && tokens[0] == "logout") {
            if (!cl->logged_in) {
                std::cout << "ERROR not logged in" << std::endl;
                continue;
            }
            std::string cmd = "LOGOUT " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
            cl->logged_in = false;
            cl->username = "";
            cl->password = "";
        }
        else if (tokens.size() >= 4 && tokens[0] == "upload" && tokens[1] == "file") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            // Reconstruct path if it contains spaces
            std::string fpath = tokens[3];
            for (size_t i = 4; i < tokens.size(); i++) {
                fpath += " " + tokens[i];
            }

            // Check if file exists
            struct stat buffer;
            if (stat(fpath.c_str(), &buffer) != 0) {
                std::cout << "ERROR local file not found: " << fpath << std::endl;
                continue;
            }

            size_t total_size = 0;
            std::vector<std::string> p_hashes = SHA1::hash_file_pieces(fpath, 524288, total_size);
            std::string file_sha1 = SHA1::hash_file(fpath);
            std::string fname = Utils::get_filename(fpath);

            std::stringstream ss;
            ss << "UPLOAD_FILE " << gid << " " << cl->username << " " << fname << " "
               << total_size << " " << file_sha1;
            for (const auto &h : p_hashes) {
                ss << " " << h;
            }

            std::string res = cl->send_to_tracker(ss.str());
            std::cout << res << std::endl;
            if (res.rfind("SUCCESS", 0) == 0) {
                cl->add_shared_file(gid, fname, fpath);
            }
        }
        else if (tokens.size() >= 3 && tokens[0] == "list" && tokens[1] == "files") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string cmd = "LIST_FILES " + gid + " " + cl->username;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
        }
        else if (tokens.size() >= 5 && tokens[0] == "download" && tokens[1] == "file") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string fname = tokens[3];
            // Reconstruct path if it has spaces
            std::string dest_path = tokens[4];
            for (size_t i = 5; i < tokens.size(); i++) {
                dest_path += " " + tokens[i];
            }

            std::string cmd = "DOWNLOAD_FILE " + gid + " " + cl->username + " " + fname;
            std::string res = cl->send_to_tracker(cmd);

            std::vector<std::string> res_tokens = Utils::split(res, ' ');
            if (res_tokens.empty() || res_tokens[0] != "SUCCESS") {
                std::cout << res << std::endl;
                continue;
            }

            // Parse: SUCCESS <size> <file_sha1> <hashes...> PEERS <peers...>
            size_t fsize = std::stoull(res_tokens[1]);
            std::string file_sha1 = res_tokens[2];

            std::vector<std::string> piece_hashes;
            std::vector<std::pair<std::string, int>> peer_addrs;

            size_t cursor = 3;
            while (cursor < res_tokens.size() && res_tokens[cursor] != "PEERS") {
                piece_hashes.push_back(res_tokens[cursor]);
                cursor++;
            }

            cursor++; // skip PEERS
            while (cursor < res_tokens.size()) {
                auto p = Utils::parse_ip_port(res_tokens[cursor]);
                if (p.second != 0) {
                    peer_addrs.push_back(p);
                }
                cursor++;
            }

            // Allocate blank file on destination path
            std::ofstream newfile(dest_path, std::ios::binary | std::ios::out);
            if (!newfile.is_open()) {
                std::cout << "ERROR cannot open destination file for allocation: " << dest_path << std::endl;
                continue;
            }
            if (fsize > 0) {
                newfile.seekp(fsize - 1);
                newfile.write("", 1);
            }
            newfile.close();

            // Setup download record
            size_t pieces_count = piece_hashes.size();
            std::vector<bool> p_status(pieces_count, false);
            cl->update_download_progress(gid, fname, 0, pieces_count, p_status, false, "Downloading");

            // Start coordinator thread
            std::thread th(download_coordinator_thread, gid, fname, dest_path, cl->username, fsize, file_sha1, piece_hashes, peer_addrs, cl);
            th.detach();

            std::cout << "SUCCESS download started" << std::endl;
        }
        else if (tokens.size() >= 2 && tokens[0] == "show" && tokens[1] == "downloads") {
            cl->show_downloads();
        }
        else if (tokens.size() >= 4 && tokens[0] == "stop" && tokens[1] == "share") {
            if (!cl->logged_in) {
                std::cout << "ERROR please login first" << std::endl;
                continue;
            }
            std::string gid = tokens[2];
            std::string fname = tokens[3];

            std::string cmd = "STOP_SHARE " + gid + " " + cl->username + " " + fname;
            std::string res = cl->send_to_tracker(cmd);
            std::cout << res << std::endl;
            if (res.rfind("SUCCESS", 0) == 0) {
                {
                    std::lock_guard<std::mutex> lock(cl->files_mtx);
                    cl->local_files.erase(gid + ":" + fname);
                }
                cl->save_local_files_db();
            }
        }
        else if (tokens[0] == "quit") {
            if (cl->logged_in) {
                std::string cmd = "LOGOUT " + cl->username;
                cl->send_to_tracker(cmd);
            }
            std::cout << "Exiting client..." << std::endl;
            break;
        }
        else {
            std::cout << "ERROR unknown command" << std::endl;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <IP>:<PORT> tracker_info.txt" << std::endl;
        return 1;
    }

    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    std::string my_address = argv[1];
    std::string tracker_info_file = argv[2];

    auto addr_pair = Utils::parse_ip_port(my_address);
    client_inst.my_ip = addr_pair.first;
    client_inst.my_port = addr_pair.second;

    if (client_inst.my_port == 0) {
        std::cerr << "Error: Invalid client address " << my_address << std::endl;
        return 1;
    }

    client_inst.trackers = Utils::parse_tracker_info(tracker_info_file);
    if (client_inst.trackers.size() < 2) {
        std::cerr << "Error: tracker_info.txt must contain details for exactly 2 trackers." << std::endl;
        return 1;
    }

    client_inst.local_files_db = "client_" + std::to_string(client_inst.my_port) + "_files.txt";
    client_inst.load_local_files_db();

    // Start local seeder server (C2C)
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Error creating C2C socket." << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Listen on all interfaces
    address.sin_port = htons(client_inst.my_port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Error: Bind failed on C2C port " << client_inst.my_port << std::endl;
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "Error listening on C2C socket." << std::endl;
        close(server_fd);
        return 1;
    }

    std::thread server_th(peer_server, server_fd, &client_inst);
    server_th.detach();

    std::cout << "Client listening for C2C connections on port " << client_inst.my_port << std::endl;
    std::cout << "Using primary tracker: " << client_inst.trackers[0].first << ":" << client_inst.trackers[0].second << std::endl;

    run_cli(&client_inst);

    close(server_fd);
    return 0;
}
