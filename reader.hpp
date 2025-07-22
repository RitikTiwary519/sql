#ifndef READER_HPP
#define READER_HPP

#include <string>
#include <string_view>
#include "TripRecord.hpp"

class Reader {
public:
    static TripRecord parseLine(const std::string& line);
    
    // Fast string parsing utilities
    static std::string_view extractField(const char* start, const char* end, char delimiter = ',');
    static double parseDouble(std::string_view sv, double defaultValue = 0.0);
    static int32_t parseInt(std::string_view sv, int32_t defaultValue = 0);
    
    // Buffer management for parallel processing
    static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB
    static constexpr size_t CHUNK_SIZE = 4 * 1024 * 1024; // 4MB chunks for parallel processing
};

#endif
