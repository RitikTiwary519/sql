#include <iostream>
#include <vector>
#include <array>
#include <immintrin.h>
#include <windows.h>
#include <iomanip>
#include <omp.h>
#include "reader.hpp"

// Structure to hold aggregated results per payment type
struct PaymentStats {
    alignas(32) size_t count = 0;  // Align for SIMD operations
    alignas(32) double fare_sum = 0.0;
    alignas(32) double tip_sum = 0.0;
};

constexpr size_t MAX_PAYMENT_TYPES = 7; // Payment types 1-6
constexpr double DISTANCE_THRESHOLD = 5.0;

void query2(const std::string& filename) {
    try {
        // Memory map the file
        HANDLE fileHandle = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, 
            nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (fileHandle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("Error opening file: " + filename);
        }

        // Thread-local statistics arrays (fixed size for payment types 1-6)
        const int MAX_PAYMENT_TYPES = 7;
        std::vector<std::vector<PaymentTypeStats>> threadStats(
            omp_get_max_threads(), 
            std::vector<PaymentTypeStats>(MAX_PAYMENT_TYPES)
        );

        // Set up buffered reading
        std::vector<char> buffer(Reader::BUFFER_SIZE);
        fin.rdbuf()->pubsetbuf(buffer.data(), Reader::BUFFER_SIZE);

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

            // Pre-load SIMD constants
            const __m256d vThreshold = _mm256_set1_pd(DISTANCE_THRESHOLD);

            while (curr < chunk_end) {
                const char* lineEnd = std::find(curr, chunk_end, '\n');
                if (lineEnd == chunk_end) break;

                std::string_view line(curr, lineEnd - curr);
                try {
                    TripRecord record = Reader::parseLine(std::string(line));
                    
                    // SIMD comparison for Trip_distance > 5.0
                    if (_mm256_movemask_pd(_mm256_cmp_pd(
                            _mm256_set1_pd(record.Trip_distance), 
                            vThreshold, 
                            _CMP_GT_OQ)) && 
                        record.Payment_type > 0 && 
                        record.Payment_type < MAX_PAYMENT_TYPES) {
                        
                        auto& stats = localStats[record.Payment_type];
                        stats.count++;
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

        // Merge results using SIMD
        std::array<PaymentStats, MAX_PAYMENT_TYPES> finalStats{};
        for (const auto& threadStat : threadStats) {
            for (size_t i = 1; i < MAX_PAYMENT_TYPES; ++i) {
                __m256i vCount = _mm256_set1_epi64x(threadStat[i].count);
                __m256d vFare = _mm256_set1_pd(threadStat[i].fare_sum);
                __m256d vTip = _mm256_set1_pd(threadStat[i].tip_sum);

                finalStats[i].count += _mm256_extract_epi64(vCount, 0);
                finalStats[i].fare_sum += _mm256_cvtsd_f64(vFare);
                finalStats[i].tip_sum += _mm256_cvtsd_f64(vTip);
            }
        }

        // Output results with formatting
        std::cout << std::fixed << std::setprecision(2);
        for (size_t i = 1; i < MAX_PAYMENT_TYPES; ++i) {
            const auto& stats = finalStats[i];
            if (stats.count > 0) {
                std::cout << "Payment_type " << i << ": "
                         << "count=" << stats.count << ", "
                         << "fare_sum=" << stats.fare_sum << ", "
                         << "tip_sum=" << stats.tip_sum << std::endl;
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
