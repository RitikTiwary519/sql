#include <windows.h>
#include <stdexcept>
#include <vector>
#include <iostream>
#include "reader.hpp"

// Function to memory-map a file on Windows
char* mmapFile(const std::string& filename, size_t& fileSize, HANDLE& fileHandle, HANDLE& fileMapping) {
    fileHandle = CreateFileA(
        filename.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );

    if (fileHandle == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("Error opening file: " + filename);
    }

    fileSize = GetFileSize(fileHandle, nullptr);
    if (fileSize == INVALID_FILE_SIZE) {
        CloseHandle(fileHandle);
        throw std::runtime_error("Error getting file size: " + filename);
    }

    fileMapping = CreateFileMappingA(
        fileHandle,
        nullptr,
        PAGE_READONLY,
        0,
        0,
        nullptr
    );

    if (!fileMapping) {
        CloseHandle(fileHandle);
        throw std::runtime_error("Error creating file mapping: " + filename);
    }

    char* data = static_cast<char*>(MapViewOfFile(
        fileMapping,
        FILE_MAP_READ,
        0,
        0,
        0
    ));

    if (!data) {
        CloseHandle(fileMapping);
        CloseHandle(fileHandle);
        throw std::runtime_error("Error mapping view of file: " + filename);
    }

    return data;
}

// Function to process a chunk of the file
void processChunk(const char* start, const char* end, std::vector<TripRecord>& records) {
    const char* curr = start;
    while (curr < end) {
        const char* lineEnd = std::find(curr, end, '\n');
        if (lineEnd == end) break;

        std::string line(curr, lineEnd - curr);
        try {
            records.push_back(Reader::parseLine(line));
        } catch (const std::exception& e) {
            std::cerr << "Error parsing line: " << e.what() << std::endl;
        }

        curr = lineEnd + 1;
    }
}

// Updated function to read and parse file using Windows memory mapping
std::vector<TripRecord> Reader::readFile(const std::string& filename) {
    size_t fileSize;
    HANDLE fileHandle = nullptr;
    HANDLE fileMapping = nullptr;
    char* data = mmapFile(filename, fileSize, fileHandle, fileMapping);

    const size_t numThreads = omp_get_max_threads();
    size_t chunkSize = fileSize / numThreads;

    std::vector<std::vector<TripRecord>> threadRecords(numThreads);

    #pragma omp parallel num_threads(numThreads)
    {
        int threadId = omp_get_thread_num();
        size_t startOffset = threadId * chunkSize;
        size_t endOffset = (threadId == numThreads - 1) ? fileSize : (threadId + 1) * chunkSize;

        // Ensure chunks start and end at newline boundaries
        if (startOffset > 0) {
            while (data[startOffset] != '\n' && startOffset < fileSize) {
                startOffset++;
            }
        }
        if (endOffset < fileSize) {
            while (data[endOffset] != '\n' && endOffset < fileSize) {
                endOffset++;
            }
        }

        processChunk(data + startOffset, data + endOffset, threadRecords[threadId]);
    }

    // Merge results from all threads
    std::vector<TripRecord> records;
    for (const auto& threadRecord : threadRecords) {
        records.insert(records.end(), threadRecord.begin(), threadRecord.end());
    }

    // Unmap the file and close handles
    UnmapViewOfFile(data);
    CloseHandle(fileMapping);
    CloseHandle(fileHandle);

    return records;
}

std::string_view Reader::extractField(const char* start, const char* end, char delimiter) {
    const char* field_end = std::find(start, end, delimiter);
    return std::string_view(start, field_end - start);
}

double Reader::parseDouble(std::string_view sv, double defaultValue) {
    double result = defaultValue;
    std::from_chars(sv.data(), sv.data() + sv.size(), result);
    return result;
}

int32_t Reader::parseInt(std::string_view sv, int32_t defaultValue) {
    int32_t result = defaultValue;
    std::from_chars(sv.data(), sv.data() + sv.size(), result);
    return result;
}

TripRecord Reader::parseLine(const std::string& line) {
    TripRecord record;
    try {
        const char* start = line.c_str();
        const char* end = start + line.length();
        const char* curr = start;

        // Extract VendorID
        auto vendorField = extractField(curr, end);
        record.VendorID = parseInt(vendorField);
        curr += vendorField.length() + 1;

        // Skip unused fields with fast pointer arithmetic
        for (int i = 0; i < 5; i++) {
            curr = std::find(curr, end, ',') + 1;
        }

        // Extract passenger_count
        auto passengerField = extractField(curr, end);
        record.passenger_count = parseInt(passengerField);
        curr = std::find(curr, end, ',') + 1;

        // Extract trip_distance
        auto distanceField = extractField(curr, end);
        record.Trip_distance = parseDouble(distanceField);
        
        // Skip more unused fields
        for (int i = 0; i < 3; i++) {
            curr = std::find(curr, end, ',') + 1;
        }

        // Extract date (fixed format YYYY-MM-DD)
        std::strncpy(record.date, curr, 10);
        curr = std::find(curr, end, ',') + 1;

        // Skip more fields to get to payment_type
        for (int i = 0; i < 5; i++) {
            curr = std::find(curr, end, ',') + 1;
        }

        // Extract payment_type
        auto paymentField = extractField(curr, end);
        record.Payment_type = parseInt(paymentField);
        curr = std::find(curr, end, ',') + 1;

        // Extract fare_amount
        auto fareField = extractField(curr, end);
        record.fare = parseDouble(fareField);
        curr = std::find(curr, end, ',') + 1;

        // Skip to tip
        curr = std::find(curr, end, ',') + 1;

        // Extract tip
        auto tipField = extractField(curr, end);
        record.tip = parseDouble(tipField);

        // Extract store_and_fwd_flag if present
        auto flagField = extractField(curr, end);
        if (!flagField.empty()) {
            record.Store_and_fwd_flag = flagField[0];
        }

    } catch (const std::exception& e) {
        std::cerr << "Malformed line: " << line.substr(0, 100) << "...\n";
    }
    return record;
}
