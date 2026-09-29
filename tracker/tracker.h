#ifndef TRACKER_H
#define TRACKER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <mutex>
#include <iostream>

struct User {
    std::string username;
    std::string password_hash;
    bool is_logged_in = false;
    std::string client_ip;
    int client_port = 0;
};

struct Group {
    std::string group_id;
    std::string owner_id;
    std::set<std::string> members;
    std::set<std::string> pending_requests;
};

struct SharedFile {
    std::string group_id;
    std::string file_name;
    size_t file_size = 0;
    std::string file_sha1;
    std::vector<std::string> piece_sha1s;
    std::set<std::string> seeders; // Usernames of seeders
};

class TrackerState {
public:
    std::mutex mtx;
    std::unordered_map<std::string, User> users;
    std::unordered_map<std::string, Group> groups;
    std::unordered_map<std::string, SharedFile> files; // key: group_id + ":" + file_name

    bool create_user(const std::string &username, const std::string &password_hash);
    bool login(const std::string &username, const std::string &password_hash, const std::string &ip, int port);
    void login_sync(const std::string &username, const std::string &ip, int port);
    bool logout(const std::string &username);
    bool create_group(const std::string &group_id, const std::string &owner_id);
    bool join_group(const std::string &group_id, const std::string &username);
    bool leave_group(const std::string &group_id, const std::string &username);
    std::vector<std::string> list_groups();
    std::vector<std::string> list_requests(const std::string &group_id, const std::string &owner_id, bool &is_owner);
    bool accept_request(const std::string &group_id, const std::string &username, const std::string &owner_id);
    bool upload_file(const std::string &group_id, const std::string &username, const std::string &file_name, size_t file_size, const std::string &file_sha1, const std::vector<std::string> &piece_sha1s);
    std::vector<std::string> list_files(const std::string &group_id, const std::string &username, bool &is_member);
    bool download_file(const std::string &group_id, const std::string &username, const std::string &file_name, size_t &file_size, std::string &file_sha1, std::vector<std::string> &piece_sha1s, std::vector<std::pair<std::string, int>> &peers);
    bool stop_share(const std::string &group_id, const std::string &username, const std::string &file_name);

    void serialize(std::ostream &os);
    void deserialize(const std::string &data);
    void save_to_file(const std::string &filepath);
    void load_from_file(const std::string &filepath);
};

#endif // TRACKER_H
