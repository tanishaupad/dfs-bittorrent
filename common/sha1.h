#ifndef SHA1_H
#define SHA1_H

#include <string>
#include <vector>
#include <cstdint>

class SHA1 {
public:
    SHA1();
    void update(const std::string &s);
    void update(const unsigned char *data, size_t len);
    std::string final();

    static std::string hash_string(const std::string &s);
    static std::string hash_file(const std::string &filepath);
    static std::vector<std::string> hash_file_pieces(const std::string &filepath, size_t piece_size, size_t &total_size);

private:
    uint32_t state[5];
    uint32_t count[2];
    unsigned char buffer[64];

    void transform(const unsigned char block[64]);
};

#endif // SHA1_H
