#include <iostream>
#include <vector>
#include <array>
#include <immintrin.h>
#include <windows.h>
#include <iomanip>
#include <omp.h>
#include "reader.hpp"

// Structure to hold vendor statistics with SIMD-friendly alignment
struct VendorStats {
    alignas(32) size_t count = 0;
    alignas(32) int32_t passenger_sum = 0;
};

// Constants for optimization
constexpr size_t MAX_VENDOR_ID = 256;  // Reasonable upper limit for vendor IDs
constexpr char TARGET_FLAG = 'Y';
constexpr char* TARGET_DATE_PREFIX = "2024-01";
constexpr size_t DATE_PREFIX_LEN = 7;

// SIMD-optimized date prefix comparison
inline bool isJanuary2024SIMD(const char* date) {
    __m128i target = _mm_loadu_si128(reinterpret_cast<const __m128i*>(TARGET_DATE_PREFIX));
    __m128i input = _mm_loadu_si128(reinterpret_cast<const __m128i*>(date));
    return (_mm_movemask_epi8(_mm_cmpeq_epi8(target, input)) & 0x7F) == 0x7F;
}

void query3(const std::string& filename) {
    try {
        // Memory map the file
        HANDLE fileHandle = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, 
            nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (fileHandle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("Error opening file: " + filename);
        }

        // Thread-local maps for vendor statistics
        std::vector<std::unordered_map<int32_t, VendorStats>> threadStats(omp_get_max_threads());

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

        // Initialize thread-local statistics arrays
        const int numThreads = omp_get_max_threads();
        std::vector<std::vector<VendorStats>> threadStats(numThreads, 
            std::vector<VendorStats>(MAX_VENDOR_ID));

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

            while (curr < chunk_end) {
                const char* lineEnd = std::find(curr, chunk_end, '\n');
                if (lineEnd == chunk_end) break;

                std::string_view line(curr, lineEnd - curr);
                try {
                    TripRecord record = Reader::parseLine(std::string(line));
                    
                    // Fast date and flag check using SIMD
                    if (record.Store_and_fwd_flag == TARGET_FLAG && 
                        isJanuary2024SIMD(record.date) &&
                        record.VendorID < MAX_VENDOR_ID) {
                        
                        auto& stats = localStats[record.VendorID];
                        stats.count++;
                        stats.passenger_sum += record.passenger_count;
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

        }

        // Merge results using SIMD
        std::vector<VendorStats> finalStats(MAX_VENDOR_ID);
        
        // Use SIMD for merging results
        for (const auto& threadStat : threadStats) {
            for (size_t i = 0; i < MAX_VENDOR_ID; i += 4) {
                __m256i vCount = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&threadStat[i].count));
                __m256i vPassenger = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&threadStat[i].passenger_sum));
                
                __m256i existingCount = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&finalStats[i].count));
                __m256i existingPassenger = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&finalStats[i].passenger_sum));
                
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(&finalStats[i].count),
                                   _mm256_add_epi64(existingCount, vCount));
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(&finalStats[i].passenger_sum),
                                   _mm256_add_epi32(existingPassenger, vPassenger));
            }
        }

        // Output results
        for (size_t i = 0; i < MAX_VENDOR_ID; ++i) {
            const auto& stats = finalStats[i];
            if (stats.count > 0) {
                std::cout << "VendorID " << i << ": "
                         << "count=" << stats.count << ", "
                         << "passenger_sum=" << stats.passenger_sum << std::endl;
            }
        }

        // Cleanup
        UnmapViewOfFile(data);
        CloseHandle(mapping);
        CloseHandle(fileHandle);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}
