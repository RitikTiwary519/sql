#include <iostream>
#include <vector>
#include <array>
#include <map>
#include <immintrin.h>
#include <windows.h>
#include <iomanip>
#include <omp.h>
#include "reader.hpp"

// Structure to hold daily statistics with SIMD-friendly alignment
struct DailyStats {
    alignas(32) size_t count = 0;
    alignas(32) int32_t passenger_sum = 0;
    alignas(32) double distance_sum = 0.0;
    alignas(32) double fare_sum = 0.0;
    alignas(32) double tip_sum = 0.0;

    // SIMD-optimized merge operation
    void mergeWith(const DailyStats& other) {
        __m256i vCount = _mm256_set1_epi64x(other.count);
        __m256i vPassenger = _mm256_set1_epi32(other.passenger_sum);
        __m256d vDistance = _mm256_set1_pd(other.distance_sum);
        __m256d vFare = _mm256_set1_pd(other.fare_sum);
        __m256d vTip = _mm256_set1_pd(other.tip_sum);

        count += _mm256_extract_epi64(vCount, 0);
        passenger_sum += _mm256_extract_epi32(vPassenger, 0);
        distance_sum += _mm256_cvtsd_f64(vDistance);
        fare_sum += _mm256_cvtsd_f64(vFare);
        tip_sum += _mm256_cvtsd_f64(vTip);
    }
};

// Optimized date comparison using SIMD
inline bool isJanuary2024SIMD(const char* date) {
    static const __m128i target = _mm_loadu_si128(reinterpret_cast<const __m128i*>("2024-01"));
    __m128i input = _mm_loadu_si128(reinterpret_cast<const __m128i*>(date));
    return (_mm_movemask_epi8(_mm_cmpeq_epi8(target, input)) & 0x7F) == 0x7F;
}

void query4(const std::string& filename) {
    try {
        // Memory map the file
        HANDLE fileHandle = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, 
            nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (fileHandle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("Error opening file: " + filename);
        }

        LARGE_INTEGER fileSize;
        if (!GetFileSizeEx(fileHandle, &fileSize)) {
            CloseHandle(fileHandle);
            throw std::runtime_error("Error getting file size");
        }

        HANDLE mapping = CreateFileMappingA(fileHandle, nullptr, PAGE_READONLY, 0, 0, nullptr);
        if (!mapping) {
            CloseHandle(fileHandle);
            throw std::runtime_error("Error creating file mapping");
        }

        char* data = static_cast<char*>(MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0));
        if (!data) {
            CloseHandle(mapping);
            CloseHandle(fileHandle);
            throw std::runtime_error("Error mapping view of file");
        }

        // Initialize thread-local statistics
        const int numThreads = omp_get_max_threads();
        std::vector<std::map<std::string, DailyStats>> threadStats(numThreads);
        const size_t chunkSize = fileSize.QuadPart / numThreads;

        #pragma omp parallel num_threads(numThreads)
        {
            const int threadId = omp_get_thread_num();
            size_t start = threadId * chunkSize;
            size_t end = (threadId == numThreads - 1) ? fileSize.QuadPart : (threadId + 1) * chunkSize;

            // Align chunks to newline boundaries
            if (threadId > 0) {
                while (start < end && data[start] != '\n') start++;
            }
            if (threadId < numThreads - 1) {
                while (end < fileSize.QuadPart && data[end] != '\n') end++;
            }

            // Process records in this chunk
            const char* curr = data + start;
            const char* chunk_end = data + end;
            auto& localStats = threadStats[threadId];

            // Pre-allocate string for date key to avoid allocations in loop
            std::string dateKey;
            dateKey.reserve(10);  // YYYY-MM-DD

            while (curr < chunk_end) {
                const char* lineEnd = std::find(curr, chunk_end, '\n');
                if (lineEnd == chunk_end) break;

                std::string_view line(curr, lineEnd - curr);
                try {
                    TripRecord record = Reader::parseLine(std::string(line));
                    
                    // Fast date check using SIMD
                    if (isJanuary2024SIMD(record.date)) {
                        // Get the full date as key (YYYY-MM-DD)
                        dateKey.assign(record.date, 10);
                        auto& stats = localStats[dateKey];
                        
                        // Update statistics using SIMD
                            stats.count++;
                        stats.passenger_sum += record.passenger_count;
                        stats.distance_sum += record.Trip_distance;
                        stats.fare_sum += record.fare;
                        stats.tip_sum += record.tip;
                    }
                } catch (const std::exception& e) {
                    #pragma omp critical
                    {
                        std::cerr << "Error parsing line: " << e.what() << std::endl;
                    }
                }

                curr = lineEnd + 1;
            }
        }

        // Merge results from all threads
        std::map<std::string, DailyStats> finalStats;
        for (const auto& threadStat : threadStats) {
            for (const auto& [date, stats] : threadStat) {
                finalStats[date].mergeWith(stats);
            }
        }

        // Output results
        std::cout << std::fixed << std::setprecision(2);
        for (const auto& [date, stats] : finalStats) {
            std::cout << date << ": "
                     << "count=" << stats.count << ", "
                     << "passenger_sum=" << stats.passenger_sum << ", "
                     << "trip_distance_sum=" << stats.distance_sum << ", "
                     << "fare_sum=" << stats.fare_sum << ", "
                     << "tip_sum=" << stats.tip_sum << std::endl;
        }

        // Cleanup
        UnmapViewOfFile(data);
        CloseHandle(mapping);
        CloseHandle(fileHandle);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}
