#ifndef CLIENT_H
#define CLIENT_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>

struct DownloadProgress {
    std::string group_id;
    std::string file_name;
    std::string destination_path;
    size_t file_size = 0;
    size_t total_pieces = 0;
    size_t downloaded_pieces = 0;
    std::vector<bool> pieces_status; // true if downloaded
    bool is_completed = false;
    bool announcing = false;
    bool peer_announced = false;
    std::string status_str = "Downloading";
};

class Client {
public:
    std::string my_ip;
    int my_port = 0;
    std::string username = "";
    std::string password = ""; // Stores password for transparent re-logins on failover
    std::atomic<bool> logged_in{false};
    std::string session_token;
    std::mutex tracker_mtx;
    std::atomic<unsigned long long> uploaded_bytes{0};

    std::vector<std::pair<std::string, int>> trackers;
    int active_tracker_idx = 0;

    // Key: group_id + ":" + file_name -> local path
    std::unordered_map<std::string, std::string> local_files;
    std::mutex files_mtx;

    // Key: group_id + ":" + file_name
    std::unordered_map<std::string, DownloadProgress> downloads;
    std::mutex downloads_mtx;

    std::string local_files_db; // file to persist uploaded file path mappings

    void load_local_files_db();
    void save_local_files_db();

    bool connect_to_tracker(int &sock);
    std::string send_to_tracker(const std::string &cmd);
    bool verify_peer_with_both_trackers(const std::string &cmd);
    std::string peer_auth_prefix();
    std::string current_username();
    void set_login_session(const std::string &user, const std::string &pwd, const std::string &token);
    void clear_login_session();

    void add_shared_file(const std::string &group_id, const std::string &file_name, const std::string &filepath);
    void announce_verified_pieces(const std::string &group_id, const std::string &file_name, const std::string &filepath, const std::string &sharing_user);
    bool get_shared_file(const std::string &group_id, const std::string &file_name, std::string &filepath);

    void update_download_progress(const std::string &group_id, const std::string &file_name, size_t downloaded, size_t total, const std::vector<bool> &status, bool completed, const std::string &status_str);
    void show_downloads();
};

#endif // CLIENT_H
