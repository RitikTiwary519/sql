#include <iostream>
#include <vector>
#include <immintrin.h>
#include <windows.h>
#include <omp.h>
#include "reader.hpp"

// Constants for SIMD processing
constexpr size_t SIMD_WIDTH = 32;  // AVX2 processes 256 bits = 32 bytes at a time
constexpr size_t CHUNK_SIZE = 64 * 1024;  // 64KB chunks for better cache utilization

// SIMD-optimized newline counter using AVX2
size_t countNewlinesSIMD(const char* data, size_t size) {
    size_t count = 0;
    const __m256i newline = _mm256_set1_epi8('\n');
    
    // Process 32 bytes at a time using AVX2
    size_t vectorized_size = size - (size % SIMD_WIDTH);
    const char* end = data + vectorized_size;
    
    for (; data < end; data += SIMD_WIDTH) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, newline);
        uint32_t mask = _mm256_movemask_epi8(cmp);
        count += _mm_popcnt_u32(mask);
    }

    // Handle remaining bytes
    for (size_t i = vectorized_size; i < size; ++i) {
        if (data[i] == '\n') count++;
    }
    
    return count;
}

// Get file size using native OS calls for better performance
size_t getFileSize(const std::string& filename) {
    struct stat st;
    if (stat(filename.c_str(), &st) != 0) {
        throw std::runtime_error("Failed to get file size");
    }
    return st.st_size;
}

void query1(const std::string& filename) {
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

        // Determine number of threads to use (hardware_concurrency or OMP_NUM_THREADS)
        int numThreads = omp_get_max_threads();
        size_t chunkSize = fileSize / numThreads;
        std::vector<FileChunk> chunks(numThreads);
        size_t totalLines = 0;

        // Parallel processing of file chunks
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

            // Count newlines in this chunk using SIMD
            counts[threadId] = countNewlinesSIMD(data + start, end - start);
        }

        // Sum up all counts
        size_t totalLines = 0;
        for (size_t count : counts) {
            totalLines += count;
        }

        // Add 1 if file doesn't end with newline
        if (fileSize.QuadPart > 0 && data[fileSize.QuadPart - 1] != '\n') {
            totalLines++;
        }

        // Cleanup
        UnmapViewOfFile(data);
        CloseHandle(mapping);
        CloseHandle(fileHandle);

        std::cout << "Total lines: " << totalLines << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}
