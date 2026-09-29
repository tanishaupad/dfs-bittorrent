#include "sha1.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cstring>

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

// blk0() and blk() perform the extension to 80 words as needed by SHA-1
#ifdef LITTLE_ENDIAN
#define blk0(i) (block[i*4] << 24 | block[i*4+1] << 16 | block[i*4+2] << 8 | block[i*4+3])
#else
#define blk0(i) (block[i*4] << 24 | block[i*4+1] << 16 | block[i*4+2] << 8 | block[i*4+3]) // Let's parse bytes manually to be safe on endianness
#endif

#define blk(i) (w[i&15] = rol(w[(i+13)&15] ^ w[(i+8)&15] ^ w[(i+2)&15] ^ w[i&15], 1))

// r0, r1, r2, r3, r4 are the SHA-1 rounds
#define r0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define r1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define r2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define r3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define r4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);

SHA1::SHA1() {
    state[0] = 0x67452301;
    state[1] = 0xEFCDAB89;
    state[2] = 0x98BADCFE;
    state[3] = 0x10325476;
    state[4] = 0xC3D2E1F0;
    count[0] = count[1] = 0;
    std::memset(buffer, 0, 64);
}

void SHA1::transform(const unsigned char block[64]) {
    uint32_t a = state[0], b = state[1], c = state[2], d = state[3], e = state[4];
    uint32_t w[16];

    // Copy block bytes into w words (big endian)
    for (int i = 0; i < 16; i++) {
        w[i] = ((uint32_t)block[i*4] << 24)
             | ((uint32_t)block[i*4+1] << 16)
             | ((uint32_t)block[i*4+2] << 8)
             | ((uint32_t)block[i*4+3]);
    }

    r0(a,b,c,d,e, 0); r0(e,a,b,c,d, 1); r0(d,e,a,b,c, 2); r0(c,d,e,a,b, 3);
    r0(b,c,d,e,a, 4); r0(a,b,c,d,e, 5); r0(e,a,b,c,d, 6); r0(d,e,a,b,c, 7);
    r0(c,d,e,a,b, 8); r0(b,c,d,e,a, 9); r0(a,b,c,d,e,10); r0(e,a,b,c,d,11);
    r0(d,e,a,b,c,12); r0(c,d,e,a,b,13); r0(b,c,d,e,a,14); r0(a,b,c,d,e,15);

    r1(e,a,b,c,d,16); r1(d,e,a,b,c,17); r1(c,d,e,a,b,18); r1(b,c,d,e,a,19);

    r2(a,b,c,d,e,20); r2(e,a,b,c,d,21); r2(d,e,a,b,c,22); r2(c,d,e,a,b,23);
    r2(b,c,d,e,a,24); r2(a,b,c,d,e,25); r2(e,a,b,c,d,26); r2(d,e,a,b,c,27);
    r2(c,d,e,a,b,28); r2(b,c,d,e,a,29); r2(a,b,c,d,e,30); r2(e,a,b,c,d,31);
    r2(d,e,a,b,c,32); r2(c,d,e,a,b,33); r2(b,c,d,e,a,34); r2(a,b,c,d,e,35);
    r2(e,a,b,c,d,36); r2(d,e,a,b,c,37); r2(c,d,e,a,b,38); r2(b,c,d,e,a,39);

    r3(a,b,c,d,e,40); r3(e,a,b,c,d,41); r3(d,e,a,b,c,42); r3(c,d,e,a,b,43);
    r3(b,c,d,e,a,44); r3(a,b,c,d,e,45); r3(e,a,b,c,d,46); r3(d,e,a,b,c,47);
    r3(c,d,e,a,b,48); r3(b,c,d,e,a,49); r3(a,b,c,d,e,50); r3(e,a,b,c,d,51);
    r3(d,e,a,b,c,52); r3(c,d,e,a,b,53); r3(b,c,d,e,a,54); r3(a,b,c,d,e,55);
    r3(e,a,b,c,d,56); r3(d,e,a,b,c,57); r3(c,d,e,a,b,58); r3(b,c,d,e,a,59);

    r4(a,b,c,d,e,60); r4(e,a,b,c,d,61); r4(d,e,a,b,c,62); r4(c,d,e,a,b,63);
    r4(b,c,d,e,a,64); r4(a,b,c,d,e,65); r4(e,a,b,c,d,66); r4(d,e,a,b,c,67);
    r4(c,d,e,a,b,68); r4(b,c,d,e,a,69); r4(a,b,c,d,e,70); r4(e,a,b,c,d,71);
    r4(d,e,a,b,c,72); r4(c,d,e,a,b,73); r4(b,c,d,e,a,74); r4(a,b,c,d,e,75);
    r4(e,a,b,c,d,76); r4(d,e,a,b,c,77); r4(c,d,e,a,b,78); r4(b,c,d,e,a,79);

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
}

void SHA1::update(const std::string &s) {
    update((const unsigned char*)s.data(), s.size());
}

void SHA1::update(const unsigned char *data, size_t len) {
    size_t i, j;
    j = (count[0] >> 3) & 63;
    if ((count[0] += len << 3) < (len << 3)) count[1]++;
    count[1] += (len >> 29);
    if ((j + len) > 63) {
        std::memcpy(&buffer[j], data, (i = 64 - j));
        transform(buffer);
        for (; i + 63 < len; i += 64) {
            transform(&data[i]);
        }
        j = 0;
    } else {
        i = 0;
    }
    std::memcpy(&buffer[j], &data[i], len - i);
}

std::string SHA1::final() {
    unsigned char finalcount[8];
    for (int i = 0; i < 8; i++) {
        finalcount[i] = (unsigned char)((count[(i >= 4 ? 0 : 1)] >> ((3 - (i & 3)) * 8)) & 255);
    }
    update((const unsigned char*)"\200", 1);
    while ((count[0] & 504) != 448) {
        update((const unsigned char*)"\0", 1);
    }
    update(finalcount, 8);

    std::stringstream ss;
    for (int i = 0; i < 5; i++) {
        ss << std::hex << std::setw(8) << std::setfill('0') << state[i];
    }
    return ss.str();
}

std::string SHA1::hash_string(const std::string &s) {
    SHA1 sha;
    sha.update(s);
    return sha.final();
}

std::string SHA1::hash_file(const std::string &filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) return "";
    SHA1 sha;
    char buf[32768];
    while (file.read(buf, sizeof(buf))) {
        sha.update((const unsigned char*)buf, file.gcount());
    }
    if (file.gcount() > 0) {
        sha.update((const unsigned char*)buf, file.gcount());
    }
    return sha.final();
}

std::vector<std::string> SHA1::hash_file_pieces(const std::string &filepath, size_t piece_size, size_t &total_size) {
    std::vector<std::string> hashes;
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        total_size = 0;
        return hashes;
    }

    file.seekg(0, std::ios::end);
    total_size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(piece_size);
    while (file) {
        file.read(buffer.data(), piece_size);
        std::streamsize bytes_read = file.gcount();
        if (bytes_read <= 0) break;

        SHA1 piece_sha;
        piece_sha.update((const unsigned char*)buffer.data(), bytes_read);
        hashes.push_back(piece_sha.final());
    }
    return hashes;
}
