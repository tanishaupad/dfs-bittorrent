#include "tracker.h"
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
#include <atomic>
#include <cstdint>
#include <cstdio>

// Global state and a durable anti-entropy journal. All command application is
// serialized by operation_mtx so snapshots and journal entries have one order.
TrackerState state;
std::mutex operation_mtx;
std::mutex journal_mtx;
std::vector<std::pair<std::string, std::string>> journal;
std::set<std::string> seen_operations;
std::atomic<bool> running{true};
std::string journal_file;
std::string state_file;
int local_tracker_no = 0;
uint64_t next_operation = 0;

std::string handle_command(const std::string &, bool, std::string &);

bool append_journal(const std::string &id, const std::string &cmd) {
    std::ofstream file(journal_file, std::ios::app);
    if (!file.is_open()) return false;
    file << id << "\t" << cmd << "\n";
    file.flush();
    return file.good();
}

void queue_sync(const std::string &cmd) {
    std::lock_guard<std::mutex> lock(journal_mtx);
    std::string id = std::to_string(local_tracker_no) + ":" +
                     std::to_string(++next_operation);
    if (!append_journal(id, cmd)) {
        std::cerr << "Failed to append tracker operation journal" << std::endl;
        return;
    }
    seen_operations.insert(id);
    journal.push_back({id, cmd});
}

void load_journal() {
    std::ifstream file(journal_file);
    std::string line;
    while (std::getline(file, line)) {
        size_t tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string id = line.substr(0, tab);
        std::string cmd = line.substr(tab + 1);
        if (seen_operations.insert(id).second) journal.push_back({id, cmd});
        std::string prefix = std::to_string(local_tracker_no) + ":";
        if (id.rfind(prefix, 0) == 0) {
            try { next_operation = std::max(next_operation, std::stoull(id.substr(prefix.size()))); }
            catch (const std::exception &) { /* Ignore malformed journal ids. */ }
        }
    }
}

bool apply_remote_operation(const std::string &id, const std::string &cmd) {
    std::lock_guard<std::mutex> operation_lock(operation_mtx);
    {
        std::lock_guard<std::mutex> journal_lock(journal_mtx);
        if (seen_operations.count(id)) return true;
    }
    std::string ignored;
    std::string response = handle_command(cmd, true, ignored);
    if (response.rfind("SUCCESS", 0) != 0) return false;
    std::lock_guard<std::mutex> journal_lock(journal_mtx);
    if (!append_journal(id, cmd)) return false;
    seen_operations.insert(id);
    journal.push_back({id, cmd});
    return true;
}

void sync_worker(std::string other_ip, int other_port) {
    while (running) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock >= 0) {
            struct timeval tv{2, 0};
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_port = htons(other_port);
            if (inet_pton(AF_INET, other_ip.c_str(), &address.sin_addr) == 1 &&
                connect(sock, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0 &&
                Utils::send_msg(sock, "SYNC_PULL")) {
                while (running) {
                    std::string response = Utils::recv_msg(sock);
                    if (response == "SYNC_END" || response.empty()) break;
                    if (response.rfind("SYNC_OP ", 0) != 0) break;
                    size_t separator = response.find(' ', 8);
                    if (separator == std::string::npos) break;
                    apply_remote_operation(response.substr(8, separator - 8),
                                           response.substr(separator + 1));
                }
                std::vector<std::pair<std::string, std::string>> snapshot;
                {
                    std::lock_guard<std::mutex> lock(journal_mtx);
                    snapshot = journal;
                }
                for (const auto &entry : snapshot) {
                    if (!Utils::send_msg(sock, "SYNC_OP " + entry.first + " " + entry.second)) break;
                    std::string reply = Utils::recv_msg(sock);
                    if (reply != "SYNC_ACK " + entry.first) break;
                }
            }
            close(sock);
        }
        for (int i = 0; i < 20 && running; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// State Operations implementations
bool TrackerState::create_user(const std::string &username, const std::string &password_hash) {
    std::lock_guard<std::mutex> lock(mtx);
    if (users.find(username) != users.end()) {
        return false;
    }
    User u;
    u.username = username;
    u.password_hash = password_hash;
    users[username] = u;
    return true;
}

bool TrackerState::login(const std::string &username, const std::string &password_hash, const std::string &ip, int port) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = users.find(username);
    if (it == users.end()) {
        return false;
    }
    if (it->second.password_hash != password_hash) {
        return false;
    }
    it->second.is_logged_in = true;
    it->second.client_ip = ip;
    it->second.client_port = port;
    return true;
}

void TrackerState::login_sync(const std::string &username, const std::string &ip, int port) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = users.find(username);
    if (it != users.end()) {
        it->second.is_logged_in = true;
        it->second.client_ip = ip;
        it->second.client_port = port;
    }
}

bool TrackerState::logout(const std::string &username) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = users.find(username);
    if (it == users.end() || !it->second.is_logged_in) {
        return false;
    }
    it->second.is_logged_in = false;
    it->second.client_ip = "";
    it->second.client_port = 0;
    return true;
}

bool TrackerState::create_group(const std::string &group_id, const std::string &owner_id) {
    std::lock_guard<std::mutex> lock(mtx);
    if (groups.find(group_id) != groups.end()) {
        return false;
    }
    Group g;
    g.group_id = group_id;
    g.owner_id = owner_id;
    g.members.insert(owner_id);
    groups[group_id] = g;
    return true;
}

bool TrackerState::join_group(const std::string &group_id, const std::string &username) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = groups.find(group_id);
    if (it == groups.end()) {
        return false;
    }
    if (it->second.members.find(username) != it->second.members.end()) {
        return false; // Already a member
    }
    it->second.pending_requests.insert(username);
    return true;
}

bool TrackerState::leave_group(const std::string &group_id, const std::string &username) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = groups.find(group_id);
    if (it == groups.end()) {
        return false;
    }
    if (it->second.members.find(username) == it->second.members.end()) {
        return false;
    }
    it->second.members.erase(username);
    return true;
}

std::vector<std::string> TrackerState::list_groups() {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::string> list;
    for (const auto &pair : groups) {
        list.push_back(pair.first);
    }
    return list;
}

std::vector<std::string> TrackerState::list_requests(const std::string &group_id, const std::string &owner_id, bool &is_owner) {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::string> list;
    auto it = groups.find(group_id);
    if (it == groups.end()) {
        is_owner = false;
        return list;
    }
    if (it->second.owner_id != owner_id) {
        is_owner = false;
        return list;
    }
    is_owner = true;
    for (const auto &req : it->second.pending_requests) {
        list.push_back(req);
    }
    return list;
}

bool TrackerState::accept_request(const std::string &group_id, const std::string &username, const std::string &owner_id) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = groups.find(group_id);
    if (it == groups.end() || it->second.owner_id != owner_id) {
        return false;
    }
    auto req_it = it->second.pending_requests.find(username);
    if (req_it == it->second.pending_requests.end()) {
        return false;
    }
    it->second.pending_requests.erase(req_it);
    it->second.members.insert(username);
    return true;
}

bool TrackerState::upload_file(const std::string &group_id, const std::string &username, const std::string &file_name, size_t file_size, const std::string &file_sha1, const std::vector<std::string> &piece_sha1s) {
    std::lock_guard<std::mutex> lock(mtx);
    auto g_it = groups.find(group_id);
    if (g_it == groups.end()) {
        return false;
    }
    if (g_it->second.members.find(username) == g_it->second.members.end()) {
        return false;
    }

    std::string key = group_id + ":" + file_name;
    auto f_it = files.find(key);
    if (f_it == files.end()) {
        SharedFile sf;
        sf.group_id = group_id;
        sf.file_name = file_name;
        sf.file_size = file_size;
        sf.file_sha1 = file_sha1;
        sf.piece_sha1s = piece_sha1s;
        sf.seeders.insert(username);
        files[key] = sf;
    } else {
        f_it->second.seeders.insert(username);
        // If piece details are missing (e.g. from seed registration), populate them
        if (f_it->second.file_size == 0 && file_size > 0) {
            f_it->second.file_size = file_size;
            f_it->second.file_sha1 = file_sha1;
            f_it->second.piece_sha1s = piece_sha1s;
        }
    }
    return true;
}

bool TrackerState::register_peer(const std::string &group_id, const std::string &username, const std::string &file_name) {
    std::lock_guard<std::mutex> lock(mtx);
    auto group = groups.find(group_id);
    auto file = files.find(group_id + ":" + file_name);
    auto user = users.find(username);
    if (group == groups.end() || file == files.end() ||
        user == users.end() || !user->second.is_logged_in ||
        group->second.members.count(username) == 0) {
        return false;
    }
    file->second.seeders.insert(username);
    return true;
}

std::vector<std::string> TrackerState::list_files(const std::string &group_id, const std::string &username, bool &is_member) {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::string> list;
    auto g_it = groups.find(group_id);
    if (g_it == groups.end() || g_it->second.members.find(username) == g_it->second.members.end()) {
        is_member = false;
        return list;
    }
    is_member = true;
    for (const auto &pair : files) {
        if (pair.second.group_id == group_id) {
            list.push_back(pair.second.file_name);
        }
    }
    return list;
}

bool TrackerState::download_file(const std::string &group_id, const std::string &username, const std::string &file_name, size_t &file_size, std::string &file_sha1, std::vector<std::string> &piece_sha1s, std::vector<std::pair<std::string, int>> &peers) {
    std::lock_guard<std::mutex> lock(mtx);
    auto g_it = groups.find(group_id);
    if (g_it == groups.end() || g_it->second.members.find(username) == g_it->second.members.end()) {
        return false;
    }
    std::string key = group_id + ":" + file_name;
    auto f_it = files.find(key);
    if (f_it == files.end()) {
        return false;
    }
    file_size = f_it->second.file_size;
    file_sha1 = f_it->second.file_sha1;
    piece_sha1s = f_it->second.piece_sha1s;

    for (const auto &seeder : f_it->second.seeders) {
        auto u_it = users.find(seeder);
        if (u_it != users.end() && u_it->second.is_logged_in) {
            peers.push_back({u_it->second.client_ip, u_it->second.client_port});
        }
    }
    return true;
}

bool TrackerState::stop_share(const std::string &group_id, const std::string &username, const std::string &file_name) {
    std::lock_guard<std::mutex> lock(mtx);
    auto g_it = groups.find(group_id);
    if (g_it == groups.end() || g_it->second.members.find(username) == g_it->second.members.end()) {
        return false;
    }
    std::string key = group_id + ":" + file_name;
    auto f_it = files.find(key);
    if (f_it == files.end()) {
        return false;
    }
    f_it->second.seeders.erase(username);
    return true;
}

void TrackerState::serialize(std::ostream &os) {
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto &pair : users) {
        os << "USER " << pair.second.username << " " << pair.second.password_hash << " "
           << (pair.second.is_logged_in ? "1" : "0") << " "
           << (pair.second.client_ip.empty() ? "-" : pair.second.client_ip)
           << " " << pair.second.client_port << "\n";
    }
    for (const auto &pair : groups) {
        os << "GROUP " << pair.second.group_id << " " << pair.second.owner_id << "\n";
        for (const auto &m : pair.second.members) {
            os << "MEMBER " << pair.second.group_id << " " << m << "\n";
        }
        for (const auto &r : pair.second.pending_requests) {
            os << "PENDING " << pair.second.group_id << " " << r << "\n";
        }
    }
    for (const auto &pair : files) {
        os << "FILE " << pair.second.group_id << " " << pair.second.file_name << " "
           << pair.second.file_size << " " << pair.second.file_sha1;
        for (const auto &p_sha : pair.second.piece_sha1s) {
            os << " " << p_sha;
        }
        os << "\n";
        for (const auto &seeder : pair.second.seeders) {
            os << "SEEDER " << pair.second.group_id << " " << pair.second.file_name << " " << seeder << "\n";
        }
    }
}

void TrackerState::deserialize(const std::string &data) {
    std::lock_guard<std::mutex> lock(mtx);
    users.clear();
    groups.clear();
    files.clear();

    std::vector<std::string> lines = Utils::split(data, '\n');
    for (auto &line : lines) {
        line = Utils::trim(line);
        if (line.empty()) continue;
        std::vector<std::string> tokens = Utils::split(line, ' ');
        if (tokens.empty()) continue;

        std::string type = tokens[0];
        if (type == "USER") {
            if (tokens.size() >= 6) {
                User u;
                u.username = tokens[1];
                u.password_hash = tokens[2];
                u.is_logged_in = (tokens[3] == "1");
                u.client_ip = tokens[4] == "-" ? "" : tokens[4];
                u.client_port = std::stoi(tokens[5]);
                users[u.username] = u;
            }
        } else if (type == "GROUP") {
            if (tokens.size() >= 3) {
                Group g;
                g.group_id = tokens[1];
                g.owner_id = tokens[2];
                groups[g.group_id] = g;
            }
        } else if (type == "MEMBER") {
            if (tokens.size() >= 3) {
                std::string g_id = tokens[1];
                std::string username = tokens[2];
                groups[g_id].members.insert(username);
            }
        } else if (type == "PENDING") {
            if (tokens.size() >= 3) {
                std::string g_id = tokens[1];
                std::string username = tokens[2];
                groups[g_id].pending_requests.insert(username);
            }
        } else if (type == "FILE") {
            if (tokens.size() >= 5) {
                SharedFile f;
                f.group_id = tokens[1];
                f.file_name = tokens[2];
                f.file_size = std::stoull(tokens[3]);
                f.file_sha1 = tokens[4];
                for (size_t i = 5; i < tokens.size(); i++) {
                    f.piece_sha1s.push_back(tokens[i]);
                }
                std::string key = f.group_id + ":" + f.file_name;
                files[key] = f;
            }
        } else if (type == "SEEDER") {
            if (tokens.size() >= 4) {
                std::string g_id = tokens[1];
                std::string f_name = tokens[2];
                std::string username = tokens[3];
                std::string key = g_id + ":" + f_name;
                files[key].seeders.insert(username);
            }
        }
    }
}

void TrackerState::save_to_file(const std::string &filepath) {
    const std::string temporary = filepath + ".tmp";
    std::ofstream file(temporary, std::ios::trunc);
    if (!file.is_open()) return;
    serialize(file);
    file.flush();
    if (!file.good()) return;
    file.close();
    if (std::rename(temporary.c_str(), filepath.c_str()) != 0)
        std::cerr << "Failed to replace tracker state file" << std::endl;
}

void TrackerState::load_from_file(const std::string &filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) return;
    std::stringstream ss;
    ss << file.rdbuf();
    deserialize(ss.str());
}

// Router for command processing
std::string handle_command(const std::string &cmd_raw, bool is_sync, std::string &associated_username) {
    std::string cmd_str = Utils::trim(cmd_raw);
    if (cmd_str.empty()) return "ERROR empty command";

    std::vector<std::string> tokens = Utils::split(cmd_str, ' ');
    if (tokens.empty()) return "ERROR empty command";

    std::string cmd = tokens[0];

    if (cmd == "CREATE_USER") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string username = tokens[1];
        std::string pwd_hash = tokens[2];
        if (state.create_user(username, pwd_hash)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR user already exists";
    }
    else if (cmd == "LOGIN") {
        if (is_sync) {
            if (tokens.size() < 4) return "ERROR invalid args";
            std::string username = tokens[1];
            std::string ip = tokens[2];
            int port = std::stoi(tokens[3]);
            state.login_sync(username, ip, port);
            state.save_to_file(state_file);
            return "SUCCESS";
        } else {
            if (tokens.size() < 5) return "ERROR invalid args";
            std::string username = tokens[1];
            std::string pwd_hash = tokens[2];
            std::string ip = tokens[3];
            int port = std::stoi(tokens[4]);
            if (state.login(username, pwd_hash, ip, port)) {
                associated_username = username;
                state.save_to_file(state_file);
                // Propagate login so other tracker knows seeder IP/Port
                queue_sync("LOGIN " + username + " " + ip + " " + std::to_string(port));
                return "SUCCESS";
            }
            return "ERROR invalid credentials or user does not exist";
        }
    }
    else if (cmd == "LOGOUT") {
        if (tokens.size() < 2) return "ERROR invalid args";
        std::string username = tokens[1];
        if (state.logout(username)) {
            if (associated_username == username) associated_username = "";
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR logout failed";
    }
    else if (cmd == "CREATE_GROUP") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        if (state.create_group(group_id, username)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR group already exists";
    }
    else if (cmd == "JOIN_GROUP") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        if (state.join_group(group_id, username)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR group not found or already a member/pending";
    }
    else if (cmd == "LEAVE_GROUP") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        if (state.leave_group(group_id, username)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR group not found or user not a member";
    }
    else if (cmd == "LIST_GROUPS") {
        std::vector<std::string> grps = state.list_groups();
        std::stringstream ss;
        ss << "SUCCESS";
        for (const auto &g : grps) {
            ss << " " << g;
        }
        return ss.str();
    }
    else if (cmd == "LIST_REQUESTS") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        bool is_owner = false;
        std::vector<std::string> reqs = state.list_requests(group_id, username, is_owner);
        if (!is_owner) {
            return "ERROR user is not the owner of this group";
        }
        std::stringstream ss;
        ss << "SUCCESS";
        for (const auto &r : reqs) {
            ss << " " << r;
        }
        return ss.str();
    }
    else if (cmd == "ACCEPT_REQUEST") {
        if (tokens.size() < 4) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        std::string owner_id = tokens[3];
        if (state.accept_request(group_id, username, owner_id)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR accept request failed";
    }
    else if (cmd == "UPLOAD_FILE") {
        if (tokens.size() < 6) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        std::string file_name = tokens[3];
        size_t file_size = std::stoull(tokens[4]);
        std::string file_sha1 = tokens[5];
        std::vector<std::string> piece_sha1s;
        for (size_t i = 6; i < tokens.size(); i++) {
            piece_sha1s.push_back(tokens[i]);
        }
        if (state.upload_file(group_id, username, file_name, file_size, file_sha1, piece_sha1s)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR upload failed (group not found or not member)";
    }
    else if (cmd == "REGISTER_SEEDER") {
        if (tokens.size() < 4) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        std::string file_name = tokens[3];
        // A peer may advertise verified pieces before completing the file.
        if (state.register_peer(group_id, username, file_name)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR register seeder failed";
    }
    else if (cmd == "LIST_FILES") {
        if (tokens.size() < 3) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        bool is_member = false;
        std::vector<std::string> files = state.list_files(group_id, username, is_member);
        if (!is_member) {
            return "ERROR user is not a member of this group";
        }
        std::stringstream ss;
        ss << "SUCCESS";
        for (const auto &f : files) {
            ss << " " << f;
        }
        return ss.str();
    }
    else if (cmd == "DOWNLOAD_FILE") {
        if (tokens.size() < 4) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        std::string file_name = tokens[3];
        size_t file_size = 0;
        std::string file_sha1;
        std::vector<std::string> piece_sha1s;
        std::vector<std::pair<std::string, int>> peers;
        if (state.download_file(group_id, username, file_name, file_size, file_sha1, piece_sha1s, peers)) {
            std::stringstream ss;
            ss << "SUCCESS " << file_size << " " << file_sha1;
            for (const auto &p_sha : piece_sha1s) {
                ss << " " << p_sha;
            }
            ss << " PEERS";
            for (const auto &p : peers) {
                ss << " " << p.first << ":" << p.second;
            }
            return ss.str();
        }
        return "ERROR download request failed (group/file not found or user not in group)";
    }
    else if (cmd == "STOP_SHARE") {
        if (tokens.size() < 4) return "ERROR invalid args";
        std::string group_id = tokens[1];
        std::string username = tokens[2];
        std::string file_name = tokens[3];
        if (state.stop_share(group_id, username, file_name)) {
            state.save_to_file(state_file);
            if (!is_sync) queue_sync(cmd_str);
            return "SUCCESS";
        }
        return "ERROR stop share failed";
    }

    return "ERROR unknown command";
}

void client_handler(int client_fd) {
    std::string associated_username = "";
    while (running) {
        std::string request = Utils::recv_msg(client_fd);
        if (request.empty()) {
            break; // Connection closed
        }

        request = Utils::trim(request);
        if (request.rfind("SYNC_OP ", 0) == 0) {
            size_t separator = request.find(' ', 8);
            if (separator == std::string::npos) {
                Utils::send_msg(client_fd, "SYNC_NACK");
                continue;
            }
            std::string id = request.substr(8, separator - 8);
            std::string cmd = request.substr(separator + 1);
            bool applied = apply_remote_operation(id, cmd);
            Utils::send_msg(client_fd, (applied ? "SYNC_ACK " : "SYNC_NACK ") + id);
        }
        else if (request == "SYNC_PULL") {
            std::vector<std::pair<std::string, std::string>> snapshot;
            {
                std::lock_guard<std::mutex> lock(journal_mtx);
                snapshot = journal;
            }
            for (const auto &entry : snapshot) {
                if (!Utils::send_msg(client_fd, "SYNC_OP " + entry.first + " " + entry.second))
                    break;
            }
            Utils::send_msg(client_fd, "SYNC_END");
        }
        else {
            std::lock_guard<std::mutex> lock(operation_mtx);
            std::string response = handle_command(request, false, associated_username);
            Utils::send_msg(client_fd, response);
        }
    }
    close(client_fd);
}

void console_handler() {
    std::string cmd;
    while (running) {
        std::cin >> cmd;
        if (cmd == "quit") {
            running = false;
            std::cout << "Shutting down tracker server..." << std::endl;
            break;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " tracker_info.txt tracker_no" << std::endl;
        return 1;
    }

    std::string tracker_info_file = argv[1];
    int tracker_no = std::stoi(argv[2]);
    if (tracker_no != 1 && tracker_no != 2) {
        std::cerr << "Error: tracker_no must be 1 or 2" << std::endl;
        return 1;
    }

    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    auto trackers = Utils::parse_tracker_info(tracker_info_file);
    if (trackers.size() < 2) {
        std::cerr << "Error: tracker_info.txt must contain details for exactly 2 trackers." << std::endl;
        return 1;
    }

    std::string my_ip = trackers[tracker_no - 1].first;
    int my_port = trackers[tracker_no - 1].second;

    std::string other_ip = trackers[2 - tracker_no].first;
    int other_port = trackers[2 - tracker_no].second;

    state_file = "tracker" + std::to_string(tracker_no) + "_state.txt";
    journal_file = "tracker" + std::to_string(tracker_no) + "_journal.txt";
    local_tracker_no = tracker_no;

    // Restore the last complete snapshot. The journal can reconstruct it if missing.
    const bool have_snapshot = std::ifstream(state_file).good();
    state.load_from_file(state_file);
    load_journal();
    if (!have_snapshot) {
        for (const auto &entry : journal) {
            std::string ignored;
            handle_command(entry.second, true, ignored);
        }
    }

    // Start synchronization worker thread
    std::thread sync_th(sync_worker, other_ip, other_port);

    // Start socket server
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Socket creation failed." << std::endl;
        running = false;
        sync_th.join();
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Listen on all interfaces
    address.sin_port = htons(my_port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed on port " << my_port << std::endl;
        close(server_fd);
        running = false;
        sync_th.join();
        return 1;
    }

    if (listen(server_fd, 20) < 0) {
        std::cerr << "Listen failed." << std::endl;
        close(server_fd);
        running = false;
        sync_th.join();
        return 1;
    }

    std::cout << "Tracker " << tracker_no << " listening on port " << my_port << std::endl;

    // Start console quit listener thread
    std::thread console_th(console_handler);

    // Set socket to non-blocking to allow checking running flag periodically
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    while (running) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd >= 0) {
            // Set timeout on client socket as well to prevent hanging threads
            struct timeval client_tv;
            client_tv.tv_sec = 600; // 10 minutes session timeout
            client_tv.tv_usec = 0;
            setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&client_tv, sizeof(client_tv));

            std::thread t(client_handler, client_fd);
            t.detach();
        }
    }

    close(server_fd);
    if (console_th.joinable()) console_th.join();
    if (sync_th.joinable()) sync_th.join();

    std::cout << "Tracker server stopped successfully." << std::endl;
    return 0;
}
